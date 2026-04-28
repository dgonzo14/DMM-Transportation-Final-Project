from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Optional, Sequence

from mta_spark_next.config import Settings, get_settings
from mta_spark_next.mta_schemas import (
    BRONZE_HISTORY_TABLE,
    CREATE_TABLE_STATEMENTS,
    GOLD_HISTORY_TABLE,
    GOLD_TABLES,
    SILVER_HISTORY_TABLE,
    SILVER_TABLES,
)
from mta_spark_next.snowflake_io import sql_in_list

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

PACKAGES = ",".join(
    [
        "net.snowflake:spark-snowflake_2.13:2.12.0-spark_3.4",
        "net.snowflake:snowflake-jdbc:3.13.30",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
)


@dataclass(frozen=True)
class SparkMTASettings:
    base: Settings
    target_database: str
    target_schema: str
    snowflake_url: str
    snowflake_private_key_file: Path
    snowflake_private_key_file_pwd: Optional[str]
    r2_endpoint: Optional[str]
    checkpoint_base: str


def _require(value: Optional[str], name: str) -> str:
    if not value:
        print(f"ERROR: required environment variable {name!r} is not set", file=sys.stderr)
        sys.exit(1)
    return value


def _identifier(name: str) -> str:
    if not IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Snowflake identifier {name!r} is not supported by this runner. "
            "Use an unquoted identifier containing letters, numbers, underscores, or dollar signs."
        )
    return name


def get_private_key_string(path: Path, password: Optional[str] = None) -> str:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    with path.open("rb") as handle:
        private_key = serialization.load_pem_private_key(
            handle.read(),
            password=password.encode("utf-8") if password else None,
            backend=default_backend(),
        )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    private_key_string = private_key_bytes.decode("utf-8")
    private_key_string = private_key_string.replace("-----BEGIN PRIVATE KEY-----", "")
    private_key_string = private_key_string.replace("-----END PRIVATE KEY-----", "")
    return private_key_string.replace("\n", "")


def _default_compare_database(base: Settings) -> str:
    source_database = _require(base.snowflake_database, "SNOWFLAKE_DATABASE")
    return source_database


def _snowflake_url(base: Settings) -> str:
    configured_url = os.getenv("SNOWFLAKE_URL")
    if configured_url:
        return configured_url
    account = _require(base.snowflake_account, "SNOWFLAKE_ACCOUNT")
    return f"{account}.snowflakecomputing.com"


def _r2_endpoint(base: Settings) -> Optional[str]:
    return os.getenv("R2_ENDPOINT") or base.r2_endpoint_url


def _checkpoint_base(base: Settings, target_database: str) -> str:
    configured = os.getenv("MTA_SPARK_CKPT_BASE") or os.getenv("CKPT_BASE")
    if configured:
        return configured.rstrip("/")

    if base.mta_sink_backend == "local":
        return str(base.repo_root / ".spark_checkpoints" / "mta_spark_next" / target_database)

    bucket = _require(base.r2_bucket, "R2_BUCKET")
    return f"s3a://{bucket}/checkpoints/mta_spark_next/{target_database}/silver"


def get_spark_mta_settings(target_database: Optional[str] = None) -> SparkMTASettings:
    base = get_settings()
    database = (
        target_database
        or os.getenv("MTA_SPARK_SNOWFLAKE_DATABASE")
        or os.getenv("MTA_SPARK_DATABASE")
        or _default_compare_database(base)
    )
    schema = os.getenv("MTA_SPARK_SNOWFLAKE_SCHEMA") or base.snowflake_schema
    key_file = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or (
        str(base.snowflake_private_key_file) if base.snowflake_private_key_file else None
    )

    return SparkMTASettings(
        base=base,
        target_database=_identifier(_require(database, "MTA_SPARK_SNOWFLAKE_DATABASE")),
        target_schema=_identifier(_require(schema, "SNOWFLAKE_SCHEMA")),
        snowflake_url=_snowflake_url(base),
        snowflake_private_key_file=Path(_require(key_file, "SNOWFLAKE_PRIVATE_KEY_FILE")).expanduser().resolve(),
        snowflake_private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
        r2_endpoint=_r2_endpoint(base),
        checkpoint_base=_checkpoint_base(base, database),
    )


def snowflake_options(settings: SparkMTASettings) -> dict[str, str]:
    options = {
        "sfURL": settings.snowflake_url,
        "sfUser": _require(settings.base.snowflake_user, "SNOWFLAKE_USER"),
        "sfDatabase": settings.target_database,
        "sfSchema": settings.target_schema,
        "sfWarehouse": _require(settings.base.snowflake_warehouse, "SNOWFLAKE_WAREHOUSE"),
        "pem_private_key": get_private_key_string(
            settings.snowflake_private_key_file,
            password=settings.snowflake_private_key_file_pwd,
        ),
    }
    if settings.base.snowflake_role:
        options["sfRole"] = settings.base.snowflake_role
    return options


def create_spark(settings: SparkMTASettings, app_name: str = "mta-spark-silver") -> SparkSession:
    from pyspark.sql import SparkSession

    cores = os.getenv("SPARK_WORKER_CORES", "*")
    master = os.getenv("MTA_SPARK_MASTER") or f"local[{cores}]"
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", PACKAGES)
    )

    if settings.base.mta_sink_backend == "r2":
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", _require(settings.r2_endpoint, "R2_ENDPOINT"))
            .config("spark.hadoop.fs.s3a.access.key", _require(settings.base.r2_access_key, "R2_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.secret.key", _require(settings.base.r2_secret_key, "R2_SECRET_KEY"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
            .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
            .config("spark.hadoop.fs.s3a.connection.ttl", "300000")
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
            .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
            .config("spark.hadoop.fs.s3a.retry.interval", "500")
            .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")
            .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def _snowflake_connector_kwargs(settings: SparkMTASettings) -> dict[str, str]:
    kwargs = {
        "account": _require(settings.base.snowflake_account, "SNOWFLAKE_ACCOUNT"),
        "user": _require(settings.base.snowflake_user, "SNOWFLAKE_USER"),
        "warehouse": _require(settings.base.snowflake_warehouse, "SNOWFLAKE_WAREHOUSE"),
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": str(settings.snowflake_private_key_file),
    }
    if settings.base.snowflake_role:
        kwargs["role"] = settings.base.snowflake_role
    if settings.snowflake_private_key_file_pwd:
        kwargs["private_key_file_pwd"] = settings.snowflake_private_key_file_pwd
    return kwargs


def ensure_compare_database_and_tables(
    settings: SparkMTASettings,
    table_names: Sequence[str] = (SILVER_HISTORY_TABLE, *SILVER_TABLES),
) -> None:
    import snowflake.connector

    database = _identifier(settings.target_database)
    schema = _identifier(settings.target_schema)

    conn = snowflake.connector.connect(**_snowflake_connector_kwargs(settings))
    cur = conn.cursor()
    try:
        cur.execute(f"USE WAREHOUSE {_identifier(_require(settings.base.snowflake_warehouse, 'SNOWFLAKE_WAREHOUSE'))}")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"USE SCHEMA {schema}")
        requested = set(table_names)
        for table_name, ddl in CREATE_TABLE_STATEMENTS.items():
            if table_name in requested:
                cur.execute(ddl)
    finally:
        cur.close()
        conn.close()


def table_fqn(settings: SparkMTASettings, table_name: str) -> str:
    return f"{settings.target_database}.{settings.target_schema}.{table_name}"


def delete_rows_by_object_keys(
    settings: SparkMTASettings,
    table_names: Sequence[str],
    object_keys: Sequence[str],
) -> None:
    import snowflake.connector

    keys = [key for key in dict.fromkeys(object_keys) if key]
    if not keys:
        return

    conn = snowflake.connector.connect(**_snowflake_connector_kwargs(settings))
    cur = conn.cursor()
    try:
        cur.execute(f"USE WAREHOUSE {_identifier(_require(settings.base.snowflake_warehouse, 'SNOWFLAKE_WAREHOUSE'))}")
        cur.execute(f"USE DATABASE {settings.target_database}")
        cur.execute(f"USE SCHEMA {settings.target_schema}")
        chunk_size = 500
        for table_name in table_names:
            for start in range(0, len(keys), chunk_size):
                chunk = keys[start : start + chunk_size]
                cur.execute(
                    f"""
                    DELETE FROM {table_fqn(settings, table_name)}
                    WHERE OBJECT_KEY IN ({sql_in_list(chunk)})
                    """
                )
    finally:
        cur.close()
        conn.close()


def delete_rows_by_values(
    settings: SparkMTASettings,
    table_name: str,
    column_name: str,
    values: Sequence[str],
) -> None:
    import snowflake.connector

    unique_values = [value for value in dict.fromkeys(values) if value is not None]
    if not unique_values:
        return

    conn = snowflake.connector.connect(**_snowflake_connector_kwargs(settings))
    cur = conn.cursor()
    try:
        cur.execute(f"USE WAREHOUSE {_identifier(_require(settings.base.snowflake_warehouse, 'SNOWFLAKE_WAREHOUSE'))}")
        cur.execute(f"USE DATABASE {settings.target_database}")
        cur.execute(f"USE SCHEMA {settings.target_schema}")
        chunk_size = 500
        for start in range(0, len(unique_values), chunk_size):
            chunk = unique_values[start : start + chunk_size]
            cur.execute(
                f"""
                DELETE FROM {table_fqn(settings, table_name)}
                WHERE {column_name} IN ({sql_in_list(chunk)})
                """
            )
    finally:
        cur.close()
        conn.close()


def write_snowflake(df: DataFrame, table_name: str, settings: SparkMTASettings, mode: str = "append") -> None:
    df = _prepare_for_snowflake_write(df, table_name)
    (
        df.write.format("net.snowflake.spark.snowflake")
        .options(**snowflake_options(settings))
        .option("dbtable", table_name)
        .option("column_mapping", "name")
        .mode(mode)
        .save()
    )


def read_snowflake_query(spark: SparkSession, settings: SparkMTASettings, query: str) -> DataFrame:
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**snowflake_options(settings))
        .option("query", query.strip())
        .load()
    )


def read_snowflake_table(spark: SparkSession, settings: SparkMTASettings, table_name: str) -> DataFrame:
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**snowflake_options(settings))
        .option("dbtable", table_name)
        .load()
    )


@lru_cache(maxsize=None)
def _table_columns(table_name: str) -> tuple[str, ...]:
    ddl = CREATE_TABLE_STATEMENTS.get(table_name)
    if not ddl:
        return ()

    body = ddl[ddl.find("(") + 1 : ddl.rfind(")")]
    columns: list[str] = []
    for raw_line in body.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue
        columns.append(line.split()[0].strip('"'))
    return tuple(columns)


def _prepare_for_snowflake_write(df: DataFrame, table_name: str) -> DataFrame:
    from pyspark.sql import functions as F

    loaded_at_tables = {BRONZE_HISTORY_TABLE, SILVER_HISTORY_TABLE, GOLD_HISTORY_TABLE, *SILVER_TABLES}
    updated_at_tables = set(GOLD_TABLES)

    if table_name in loaded_at_tables and "LOADED_AT" not in df.columns:
        df = df.withColumn("LOADED_AT", F.current_timestamp())
    if table_name in updated_at_tables and "UPDATED_AT" not in df.columns:
        df = df.withColumn("UPDATED_AT", F.current_timestamp())

    table_columns = _table_columns(table_name)
    if not table_columns:
        return df

    ordered_columns = [column for column in table_columns if column in df.columns]
    extra_columns = [column for column in df.columns if column not in ordered_columns]
    return df.select(*ordered_columns, *extra_columns)


def bronze_base_path(settings: SparkMTASettings, bronze_type: str) -> str:
    prefix = settings.base.mta_bronze_prefix.strip("/")
    if settings.base.mta_sink_backend == "local":
        return str(settings.base.mta_local_base_path / prefix / bronze_type)

    bucket = _require(settings.base.r2_bucket, "R2_BUCKET")
    return f"s3a://{bucket}/{prefix}/{bronze_type}/"


def bronze_day_paths(settings: SparkMTASettings, bronze_type: str, start: date, end: date) -> List[str]:
    paths: List[str] = []
    current = start
    base = bronze_base_path(settings, bronze_type).rstrip("/")
    while current <= end:
        if settings.base.mta_sink_backend == "local":
            paths.append(
                str(
                    Path(base)
                    / f"year={current.year}"
                    / f"month={current.month:02d}"
                    / f"day={current.day:02d}"
                )
            )
        else:
            paths.append(
                f"{base}/year={current.year}/month={current.month:02d}/day={current.day:02d}/"
            )
        current += timedelta(days=1)
    return paths


def read_bronze_text_stream(
    spark: SparkSession,
    base_path: str,
    max_files_per_trigger: int,
) -> DataFrame:
    return (
        spark.readStream.format("text")
        .option("wholetext", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(base_path)
    )


def read_bronze_text_batch(spark: SparkSession, paths: Iterable[str]) -> DataFrame:
    return (
        spark.read.format("text")
        .option("wholetext", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .load(list(paths))
    )
