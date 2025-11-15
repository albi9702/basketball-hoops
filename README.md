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
   Copy the `.env.example` file to `.env` and fill in the necessary values.

## Usage Guidelines
- To run the main scraping script, execute:
  ```
  python src/main.py
  ```

- For exploratory data analysis, open the Jupyter notebook located in the `notebooks` directory:
  ```
  jupyter notebook notebooks/exploration.ipynb
  ```

## Directory Structure
- `data/`: Contains raw and processed data.
- `notebooks/`: Jupyter notebooks for data exploration.
- `src/`: Main source code for scraping and data storage.
- `tests/`: Unit tests for the project.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.