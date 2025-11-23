# basketball-hoops

## Overview
The "basketball-hoops" project is designed to scrape data from Basketball Reference, focusing on international basketball leagues. The project aims to collect, process, and store basketball data for analysis and visualization.

## Setup Instructions
1. **Clone the repository:**
   ```
   git clone https://github.com/yourusername/basketball-hoops.git
   cd basketball-hoops
   ```

2. **Create a virtual environment:**
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install the required dependencies:**
   ```
   pip install -r requirements.txt
   ```

4. **Set up environment variables:**
   Copy the `.env.example` file to `.env` and fill in the necessary values. By
   default the scraper writes to a local SQLite database at
   `data/basketball_hoops.db`, which you can inspect with any VS Code database
   extension. To switch to PostgreSQL, provide a valid `DATABASE_URL`, for example:
### Provisioning PostgreSQL locally

If you prefer running PostgreSQL instead of SQLite, execute the helper script (it
will install Docker automatically on Debian/Ubuntu or Fedora if it isn’t already
available):

```bash
./scripts/setup_postgres.sh
```

It will start a Dockerized PostgreSQL 16 instance (or reuse the existing
`basketball-hoops-db` container), create the `basketball_hoops` schema, and update
your `.env` with the proper `DATABASE_URL` and table names. Customize usernames,
passwords, or ports by exporting `POSTGRES_USER`, `POSTGRES_PASSWORD`, etc., before
running the script.


   ```env
   DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/basketball
   DATABASE_SCHEMA=basketball_hoops
   DATABASE_SCHEDULE_TABLE=schedule_games
   DATABASE_BOXSCORE_TABLE=boxscores
   DATABASE_SEASON_TABLE=seasons
   ```

## Usage Guidelines
- To run the main scraping script, execute:
   ```
   python -m src.main --mode full
   ```

- Daily delta scrapes (only store a specific game date) can be triggered with:
   ```
   python -m src.main --mode daily --target-date 2025-11-23
   ```
   If `--target-date` is omitted in daily mode, the script defaults to the current UTC date. Schedules are filtered before boxscores are collected, and schedule rows are appended instead of replaced.

- For exploratory data analysis, open the Jupyter notebook located in the `notebooks` directory:
   ```
   jupyter notebook notebooks/exploration.ipynb
   ```

## Directory Structure
- `data/`: Contains raw and processed data.
- `notebooks/`: Jupyter notebooks for data exploration.
- `src/`: Main source code for scraping and data storage. Database-related settings
   live in `src/configs/settings.py` (edit the schema/table names there to match your
   environment).
- `tests/`: Unit tests for the project.

## Database output
When `python src/main.py` runs, the scraper persists the season, schedule, and
boxscore DataFrames to the configured relational database. Out of the box it uses a
local SQLite file (`data/basketball_hoops.db`). If you set `DATABASE_URL` to
PostgreSQL (or any SQLAlchemy-compatible database) but the connection fails, the
app automatically falls back to the local SQLite file and logs a warning so you can
fix credentials later. You can change the schema or table names in
`DatabaseConfig` inside `src/configs/settings.py`, making it easy to point BI tools
or VS Code database extensions at the generated tables to inspect row counts.

## Scheduled runs via GitHub Actions
Two workflows automate scraping in CI:

- `.github/workflows/daily-scrape.yml` runs every day at 05:15 UTC (and supports manual dispatch). It resolves the current date and executes `python -m src.main --mode daily --target-date <today>` so that only new games for the day are appended to the database.
- `.github/workflows/weekly-full-scrape.yml` runs every Monday at 06:00 UTC (plus manual dispatch) and launches a full refresh via `python -m src.main --mode full`.

Both workflows require the `DATABASE_URL` secret to be configured in the repository settings so that the scraper can reach your production database. Add any additional secrets (e.g., cloud provider credentials) as needed, and adjust the cron expressions to match your operational windows.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.