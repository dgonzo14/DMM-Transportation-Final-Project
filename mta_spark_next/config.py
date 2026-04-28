from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv


REPO_MARKERS = (".git", "requirements.txt", "README.md")


def _candidate_roots(start: Path) -> List[Path]:
    current = start.resolve()
    if current.is_file():
        current = current.parent
    return [current, *current.parents]


def find_repo_root() -> Path:
    override = os.getenv("MTA_REPO_ROOT")
    if override:
        return Path(override).expanduser().resolve()

    seen = set()
    for origin in (Path.cwd(), Path(__file__).resolve()):
        for candidate in _candidate_roots(origin):
            if candidate in seen:
                continue
            seen.add(candidate)
            score = sum(1 for marker in REPO_MARKERS if (candidate / marker).exists())
            if (candidate / ".git").exists() or score >= 2:
                return candidate

    return Path(__file__).resolve().parents[1]


def resolve_repo_path(raw_path: Optional[str], repo_root: Path) -> Optional[Path]:
    if not raw_path:
        return None
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def snowflake_database_for_spark() -> Optional[str]:
    compare_database = os.getenv("MTA_SPARK_SNOWFLAKE_DATABASE") or os.getenv("MTA_SPARK_DATABASE")
    if compare_database:
        return compare_database

    source_database = os.getenv("SNOWFLAKE_DATABASE")
    if source_database:
        return f"{source_database}_SPARK_COMPARE"
    return None


@dataclass(frozen=True)
class Settings:
    repo_root: Path
    env_path: Optional[Path]
    mta_poll_interval: int
    mta_subway_feeds_file: Path
    mta_sink_backend: str
    mta_local_base_path: Path
    mta_bronze_prefix: str
    r2_bucket: Optional[str]
    r2_account_id: Optional[str]
    r2_access_key: Optional[str]
    r2_secret_key: Optional[str]
    snowflake_account: Optional[str]
    snowflake_user: Optional[str]
    snowflake_warehouse: Optional[str]
    snowflake_database: Optional[str]
    snowflake_schema: Optional[str]
    snowflake_role: Optional[str]
    snowflake_private_key_file: Optional[Path]
    snowflake_private_key_file_pwd: Optional[str]

    @property
    def r2_endpoint_url(self) -> Optional[str]:
        if not self.r2_account_id:
            return None
        return f"https://{self.r2_account_id}.r2.cloudflarestorage.com"

    @property
    def snowflake_enabled(self) -> bool:
        required = [
            self.snowflake_account,
            self.snowflake_user,
            self.snowflake_warehouse,
            self.snowflake_database,
            self.snowflake_schema,
            self.snowflake_private_key_file,
        ]
        return all(required)

    def load_feeds(self) -> List[Dict[str, str]]:
        if not self.mta_subway_feeds_file.exists():
            raise FileNotFoundError(
                f"MTA feeds file not found at {self.mta_subway_feeds_file}. "
                "Set MTA_SUBWAY_FEEDS_FILE or MTA_SUBWAY_FEEDS_JSON to a valid path."
            )

        with self.mta_subway_feeds_file.open("r", encoding="utf-8") as handle:
            feeds = json.load(handle)

        if not isinstance(feeds, list) or not feeds:
            raise ValueError("Feeds file must contain a non-empty JSON list.")

        normalized_feeds: List[Dict[str, str]] = []
        for idx, item in enumerate(feeds):
            if not isinstance(item, dict):
                raise ValueError(f"Feed entry {idx} must be an object.")
            feed_name = item.get("feed_name")
            url = item.get("url")
            if not feed_name or not url:
                raise ValueError(f"Feed entry {idx} must contain feed_name and url.")
            normalized_feeds.append({"feed_name": str(feed_name), "url": str(url)})

        return normalized_feeds


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    repo_root = find_repo_root()
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

    feeds_value = os.getenv("MTA_SUBWAY_FEEDS_FILE") or os.getenv("MTA_SUBWAY_FEEDS_JSON", "feeds.json")
    settings = Settings(
        repo_root=repo_root,
        env_path=env_path if env_path.exists() else None,
        mta_poll_interval=int(os.getenv("MTA_POLL_INTERVAL", "30")),
        mta_subway_feeds_file=resolve_repo_path(feeds_value, repo_root) or (repo_root / "feeds.json"),
        mta_sink_backend=os.getenv("MTA_SINK_BACKEND", "r2").strip().lower(),
        mta_local_base_path=resolve_repo_path(os.getenv("MTA_LOCAL_BASE_PATH", "./landing_zone"), repo_root)
        or (repo_root / "landing_zone"),
        mta_bronze_prefix=os.getenv("MTA_BRONZE_PREFIX", "bronze/mta").strip("/"),
        r2_bucket=os.getenv("R2_BUCKET"),
        r2_account_id=os.getenv("R2_ACCOUNT_ID"),
        r2_access_key=os.getenv("R2_ACCESS_KEY"),
        r2_secret_key=os.getenv("R2_SECRET_KEY"),
        snowflake_account=os.getenv("SNOWFLAKE_ACCOUNT"),
        snowflake_user=os.getenv("SNOWFLAKE_USER"),
        snowflake_warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        snowflake_database=snowflake_database_for_spark(),
        snowflake_schema=os.getenv("SNOWFLAKE_SCHEMA"),
        snowflake_role=os.getenv("SNOWFLAKE_ROLE"),
        snowflake_private_key_file=resolve_repo_path(os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"), repo_root),
        snowflake_private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
    )

    if settings.mta_sink_backend not in {"local", "r2"}:
        raise ValueError("MTA_SINK_BACKEND must be either 'local' or 'r2'.")

    return settings
