"""Entry point for scraping Basketball Reference seasons and league schedules."""

import pandas as pd

from src.scraping.basketball_reference_scraper import BasketballReferenceScraper

# from src.storage.local_store import save_to_local
# from src.storage.cloud_store import save_to_cloud


def main():
    scraper = BasketballReferenceScraper()

    # Step 1: Scrape the international seasons overview
    seasons_df = scraper.scrape().head()
    print(f"Scraped {len(seasons_df)} season rows")

    # Step 2: Scrape every available league schedule using the generated URLs
    schedules_df = scraper.scrape_league_schedules(seasons_df)
    if not schedules_df.empty:
        league_count = schedules_df['League'].nunique()
        print(
            f"Scraped {len(schedules_df)} schedule rows across {league_count} leagues"
        )
    else:
        print("No schedule URLs were available.")

    # Step 3: Scrape detailed box score tables for each game
    boxscores_df = scraper.scrape_boxscore_tables(schedules_df)
    if not boxscores_df.empty:
        print(f"Scraped {len(boxscores_df)} boxscore rows")
    else:
        print("No boxscore data was available.")

    # Step 4: Save the raw data locally / to the cloud as needed
    # save_to_local(seasons_df, 'data/raw/international_seasons.csv')
    # save_to_local(schedules_df, 'data/raw/league_schedules.csv')
    # save_to_local(boxscores_df, 'data/raw/boxscores.csv')
    # save_to_cloud(seasons_df, 'international_seasons')
    # save_to_cloud(schedules_df, 'league_schedules')
    # save_to_cloud(boxscores_df, 'boxscores')


if __name__ == "__main__":
    main()