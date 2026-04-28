from __future__ import annotations

import argparse

from mta_spark_next.mta_common import ensure_compare_database_and_tables, get_spark_mta_settings
from mta_spark_next.mta_schemas import CREATE_TABLE_STATEMENTS


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the MTA Spark compare database and table schemas.")
    parser.add_argument("--target-database", help="Override MTA_SPARK_SNOWFLAKE_DATABASE for this run.")
    parser.add_argument(
        "--silver-only",
        action="store_true",
        help="Create only the silver tables used by the Spark runner.",
    )
    args = parser.parse_args()

    settings = get_spark_mta_settings(target_database=args.target_database)
    if args.silver_only:
        from mta_spark_next.mta_schemas import SILVER_HISTORY_TABLE, SILVER_TABLES

        table_names = [SILVER_HISTORY_TABLE, *SILVER_TABLES]
    else:
        table_names = list(CREATE_TABLE_STATEMENTS)

    ensure_compare_database_and_tables(settings, table_names=table_names)
    print(f"[mta-spark] prepared {settings.target_database}.{settings.target_schema}")


if __name__ == "__main__":
    main()
