"""Entry point for scraping Basketball Reference seasons and league schedules."""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from typing import Optional

from src.configs.logging_config import configure_logging, get_logger
from src.scraping.basketball_reference_scraper import BasketballReferenceScraper
from src.storage.database_store import DatabaseStore

logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Basketball Reference data in full refresh or daily delta mode.",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "daily"],
        default="full",
        help="Full mode refreshes every season. Daily mode limits the scrape to a single date.",
    )
    parser.add_argument(
        "--target-date",
        help="YYYY-MM-DD value to filter the schedules/boxscores (defaults to today for daily mode).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO).",
    )
    parser.add_argument(
        "--log-file",
        default="logs/scraper.log",
        help="Path to log file (default: logs/scraper.log).",
    )
    return parser.parse_args()


def _resolve_target_date(mode: str, value: Optional[str]) -> Optional[date]:
    if value:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise SystemExit(f"Invalid --target-date '{value}': {exc}")
    if mode == "daily":
        return datetime.utcnow().date()
    return None


def main(mode: str = "full", target_date: Optional[date] = None) -> None:
    logger.info("Starting scraper in %s mode", mode)
    if target_date:
        logger.info("Target date: %s", target_date.isoformat())

    scraper = BasketballReferenceScraper()
    try:
        store: DatabaseStore | None = DatabaseStore()
        logger.info("Database connection established")
    except RuntimeError as exc:
        logger.warning("Database persistence disabled: %s", exc)
        store = None

    seasons_df = scraper.scrape().head(10)
    logger.info("Scraped %d season rows", len(seasons_df))

    schedules_df = scraper.scrape_league_schedules(seasons_df)
    if target_date:
        before = len(schedules_df)
        schedules_df = scraper.filter_schedule_by_date(schedules_df, target_date)
        logger.info(
            "Filtered schedules to %d rows for %s (from %d)",
            len(schedules_df),
            target_date.isoformat(),
            before,
        )

    if not schedules_df.empty:
        league_count = schedules_df['League'].nunique()
        logger.info(
            "Scraped %d schedule rows across %d leagues",
            len(schedules_df),
            league_count,
        )
    else:
        logger.warning("No schedule rows matched the requested criteria.")

    boxscores_df = scraper.scrape_boxscore_tables(schedules_df, store=store)
    if not boxscores_df.empty:
        logger.info("Scraped %d boxscore rows", len(boxscores_df))
        if store:
            logger.info(
                "Stored boxscore rows incrementally in %s.%s",
                store.schema or "default",
                store.boxscore_table,
            )
    else:
        logger.warning("No boxscore data was available.")

    if store and mode == "full" and not seasons_df.empty:
        saved = store.save_seasons(seasons_df)
        logger.info(
            "Stored %d season rows in %s.%s",
            saved,
            store.schema or "default",
            store.season_table,
        )

    if store and not schedules_df.empty:
        if_exists = "replace" if mode == "full" else "append"
        saved = store.save_schedules(schedules_df, if_exists=if_exists)
        logger.info(
            "Stored %d schedule rows in %s.%s using if_exists=%s",
            saved,
            store.schema or "default",
            store.schedule_table,
            if_exists,
        )

    logger.info("Scrape completed successfully")


if __name__ == "__main__":
    cli_args = _parse_args()
    configure_logging(level=cli_args.log_level, log_file=cli_args.log_file)
    main(mode=cli_args.mode, target_date=_resolve_target_date(cli_args.mode, cli_args.target_date))