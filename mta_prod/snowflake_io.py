from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from mta_prod.config import Settings
from mta_prod.mta_schemas import CREATE_TABLE_STATEMENTS


LOGGER = logging.getLogger(__name__)


def get_snowflake_connection(settings: Settings):
    missing = [
        key for key, value in {
            "SNOWFLAKE_ACCOUNT": settings.snowflake_account,
            "SNOWFLAKE_USER": settings.snowflake_user,
            "SNOWFLAKE_WAREHOUSE": settings.snowflake_warehouse,
            "SNOWFLAKE_DATABASE": settings.snowflake_database,
            "SNOWFLAKE_SCHEMA": settings.snowflake_schema,
            "SNOWFLAKE_PRIVATE_KEY_FILE": settings.snowflake_private_key_file,
        }.items() if not value
    ]
    if missing:
        raise ValueError(f"Missing Snowflake configuration: {missing}")

    kwargs = {
        "account": settings.snowflake_account,
        "user": settings.snowflake_user,
        "warehouse": settings.snowflake_warehouse,
        "database": settings.snowflake_database,
        "schema": settings.snowflake_schema,
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": str(settings.snowflake_private_key_file),
    }
    if settings.snowflake_role:
        kwargs["role"] = settings.snowflake_role
    if settings.snowflake_private_key_file_pwd:
        kwargs["private_key_file_pwd"] = settings.snowflake_private_key_file_pwd

    conn = snowflake.connector.connect(**kwargs)
    cur = conn.cursor()
    try:
        cur.execute(f"USE WAREHOUSE {settings.snowflake_warehouse}")
        cur.execute(f"USE DATABASE {settings.snowflake_database}")
        cur.execute(f"USE SCHEMA {settings.snowflake_schema}")
    finally:
        cur.close()
    return conn


def table_fqn(settings: Settings, table_name: str) -> str:
    return f"{settings.snowflake_database}.{settings.snowflake_schema}.{table_name}"


def ensure_tables_exist(conn, table_names: Optional[Sequence[str]] = None) -> None:
    cur = conn.cursor()
    try:
        names = set(table_names) if table_names else set(CREATE_TABLE_STATEMENTS)
        for table_name, ddl in CREATE_TABLE_STATEMENTS.items():
            if table_name in names:
                cur.execute(ddl)
    finally:
        cur.close()


def fetch_dataframe(conn, sql: str) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_in_list(values: Iterable[str]) -> str:
    return ", ".join(sql_quote(value) for value in values)


def delete_rows_by_values(
    conn,
    settings: Settings,
    table_name: str,
    column_name: str,
    values: Sequence[str],
) -> None:
    unique_values = [value for value in dict.fromkeys(values) if value is not None]
    if not unique_values:
        return

    cur = conn.cursor()
    try:
        chunk_size = 500
        for start in range(0, len(unique_values), chunk_size):
            chunk = unique_values[start:start + chunk_size]
            cur.execute(
                f"""
                DELETE FROM {table_fqn(settings, table_name)}
                WHERE {column_name} IN ({sql_in_list(chunk)})
                """
            )
    finally:
        cur.close()


def write_dataframe(conn, settings: Settings, df: pd.DataFrame, table_name: str) -> None:
    if df.empty:
        return

    write_pandas(
        conn,
        df,
        table_name,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
        quote_identifiers=False,
        use_logical_type=True,
    )


def append_history_rows(
    conn,
    settings: Settings,
    table_name: str,
    rows: List[Dict[str, object]],
) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    write_dataframe(conn, settings, df, table_name)


def fetch_success_object_keys(
    conn,
    settings: Settings,
    history_table: str,
    data_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[str]:
    filters = ["STATUS = 'SUCCESS'"]
    if data_type:
        filters.append(f"DATA_TYPE = {sql_quote(data_type)}")
    if start_date:
        filters.append(f"CAST(INGESTED_AT AS DATE) >= {sql_quote(start_date)}")
    if end_date:
        filters.append(f"CAST(INGESTED_AT AS DATE) <= {sql_quote(end_date)}")

    sql = f"""
        SELECT DISTINCT OBJECT_KEY
        FROM {table_fqn(settings, history_table)}
        WHERE {' AND '.join(filters)}
    """
    df = fetch_dataframe(conn, sql)
    if df.empty:
        return []
    return [str(value) for value in df["OBJECT_KEY"].dropna().tolist()]


def fetch_max_ingested_at(
    conn,
    settings: Settings,
    history_table: str,
    data_type: Optional[str] = None,
) -> Optional[pd.Timestamp]:
    filters = ["STATUS = 'SUCCESS'"]
    if data_type:
        filters.append(f"DATA_TYPE = {sql_quote(data_type)}")

    df = fetch_dataframe(
        conn,
        f"""
        SELECT MAX(INGESTED_AT) AS MAX_INGESTED_AT
        FROM {table_fqn(settings, history_table)}
        WHERE {' AND '.join(filters)}
        """,
    )
    if df.empty:
        return None
    value = df.iloc[0]["MAX_INGESTED_AT"]
    if pd.isna(value):
        return None
    return pd.to_datetime(value, utc=True)
