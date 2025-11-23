"""Entry point for scraping Basketball Reference seasons and league schedules."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Optional

from src.scraping.basketball_reference_scraper import BasketballReferenceScraper
from src.storage.database_store import DatabaseStore


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
    scraper = BasketballReferenceScraper()
    try:
        store: DatabaseStore | None = DatabaseStore()
    except RuntimeError as exc:
        print("Warning: database persistence disabled because the connection failed.")
        print(f"Reason: {exc}")
        store = None

    seasons_df = scraper.scrape().head(10)
    print(f"Scraped {len(seasons_df)} season rows")

    schedules_df = scraper.scrape_league_schedules(seasons_df)
    if target_date:
        before = len(schedules_df)
        schedules_df = scraper.filter_schedule_by_date(schedules_df, target_date)
        print(
            f"Filtered schedules to {len(schedules_df)} rows for {target_date.isoformat()}"
            f" (from {before})"
        )

    if not schedules_df.empty:
        league_count = schedules_df['League'].nunique()
        print(
            f"Scraped {len(schedules_df)} schedule rows across {league_count} leagues"
        )
    else:
        print("No schedule rows matched the requested criteria.")

    boxscores_df = scraper.scrape_boxscore_tables(schedules_df, store=store)
    if not boxscores_df.empty:
        print(f"Scraped {len(boxscores_df)} boxscore rows")
        if store:
            print(
                "Stored boxscore rows incrementally in "
                f"{store.schema or 'default'}.{store.boxscore_table}"
            )
    else:
        print("No boxscore data was available.")

    if store and mode == "full" and not seasons_df.empty:
        saved = store.save_seasons(seasons_df)
        print(
            f"Stored {saved} season rows in {store.schema or 'default'}.{store.season_table}"
        )

    if store and not schedules_df.empty:
        if_exists = "replace" if mode == "full" else "append"
        saved = store.save_schedules(schedules_df, if_exists=if_exists)
        print(
            f"Stored {saved} schedule rows in {store.schema or 'default'}.{store.schedule_table}"
            f" using if_exists={if_exists}"
        )


if __name__ == "__main__":
    cli_args = _parse_args()
    main(mode=cli_args.mode, target_date=_resolve_target_date(cli_args.mode, cli_args.target_date))