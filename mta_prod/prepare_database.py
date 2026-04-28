from __future__ import annotations

import argparse

from mta_prod.mta_common import ensure_database_and_tables, get_spark_mta_settings
from mta_prod.mta_schemas import CREATE_TABLE_STATEMENTS


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the MTA production Snowflake database and table schemas.")
    parser.add_argument("--target-database", help="Optional one-off Snowflake database override. Defaults to SNOWFLAKE_DATABASE.")
    parser.add_argument(
        "--silver-only",
        action="store_true",
        help="Create only the silver tables used by the Spark runner.",
    )
    args = parser.parse_args()

    settings = get_spark_mta_settings(target_database=args.target_database)
    if args.silver_only:
        from mta_prod.mta_schemas import SILVER_HISTORY_TABLE, SILVER_TABLES

        table_names = [SILVER_HISTORY_TABLE, *SILVER_TABLES]
    else:
        table_names = list(CREATE_TABLE_STATEMENTS)

    ensure_database_and_tables(settings, table_names=table_names)
    print(f"[mta-prod] prepared {settings.target_database}.{settings.target_schema}")


if __name__ == "__main__":
    main()
