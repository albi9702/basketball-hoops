# Contents of /basketball-hoops/basketball-hoops/src/configs/settings.py

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration settings
class Config:
    BASE_URL = "https://www.basketball-reference.com"
    DATA_DIR = os.path.join(os.path.dirname(__file__), '../../data')
    RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
    PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
    API_KEY = os.getenv("API_KEY")  # Example for future API usage
    TIMEOUT = 10  # Timeout for requests in seconds

# Example of how to use the configuration
if __name__ == "__main__":
    print("Base URL:", Config.BASE_URL)
    print("Raw Data Directory:", Config.RAW_DATA_DIR)
    print("Processed Data Directory:", Config.PROCESSED_DATA_DIR)