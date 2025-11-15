# main.py

import pandas as pd
from src.scraping.basketball_reference_scraper import scrape_data
from src.storage.local_store import save_to_local
from src.storage.cloud_store import save_to_cloud

def main():
    # Step 1: Scrape data from Basketball Reference
    data = scrape_data()

    # Step 2: Save the raw data locally
    save_to_local(data, 'data/raw/basketball_data.csv')

    # Step 3: Optionally, save the data to cloud storage
    save_to_cloud(data, 'basketball_data')

if __name__ == "__main__":
    main()