from __future__ import annotations

import argparse
import logging
from typing import Any, Dict, Optional

from mta_next.mta_arrival_inference import run_mta_gold_inference_once
from mta_next.mta_pipeline import BRONZE_TYPES, run_mta_silver_pipeline_once
from mta_next.utils import utc_now


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def run_mta_backfill(
    start_date: Optional[str],
    end_date: Optional[str],
    bronze_type: str,
    layers: str,
    mode: str,
    max_objects: Optional[int] = None,
) -> Dict[str, Any]:
    dag_run_id = f"backfill__{utc_now().isoformat()}"
    results: Dict[str, Any] = {
        "dag_run_id": dag_run_id,
        "bronze_type": bronze_type,
        "layers": layers,
        "mode": mode,
    }

    authoritative_bronze_type = bronze_type
    if bronze_type == "all":
        authoritative_bronze_type = "full_feed"
        LOGGER.info(
            "Backfill requested bronze_type=all. Using full_feed as the authoritative bronze type "
            "for the production bronze -> silver -> gold path."
        )

    if layers in {"silver", "all"}:
        results["silver"] = run_mta_silver_pipeline_once(
            dag_run_id=dag_run_id,
            bronze_type=authoritative_bronze_type,
            start_date=start_date,
            end_date=end_date,
            force=True,
            max_objects=max_objects,
        )

    if layers in {"gold", "all"}:
        results["gold"] = run_mta_gold_inference_once(
            dag_run_id=dag_run_id,
            start_date=start_date,
            end_date=end_date,
            force=True,
            max_objects=max_objects,
        )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical MTA bronze -> silver -> gold backfill.")
    parser.add_argument("--start-date", help="Inclusive start date filter in YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Inclusive end date filter in YYYY-MM-DD.")
    parser.add_argument(
        "--bronze-type",
        choices=[*BRONZE_TYPES, "all"],
        default="full_feed",
        help="Bronze object type to replay. Use all to follow the authoritative full_feed production path.",
    )
    parser.add_argument(
        "--layers",
        choices=["silver", "gold", "all"],
        default="all",
        help="Backfill only silver, only gold, or both.",
    )
    parser.add_argument(
        "--mode",
        choices=["merge", "overwrite"],
        default="merge",
        help="Both modes are safe to rerun; overwrite still uses targeted delete+insert by impacted keys.",
    )
    parser.add_argument("--max-objects", type=int, help="Optional cap for one-shot backfill testing.")
    args = parser.parse_args()

    results = run_mta_backfill(
        start_date=args.start_date,
        end_date=args.end_date,
        bronze_type=args.bronze_type,
        layers=args.layers,
        mode=args.mode,
        max_objects=args.max_objects,
    )
    LOGGER.info("MTA backfill summary: %s", results)


if __name__ == "__main__":
    main()
