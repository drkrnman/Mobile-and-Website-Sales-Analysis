# Mobile app and Website Sales Analysis

A project for analyzing sales through a mobile application and website.

## Features
- ETL and EDA in Jupyter notebooks (`/notebooks/etl.ipynb`, `/notebooks/eda.ipynb`).
- Tkinter GUI for statistical tests (t-test, Chi-square) on key metrics.
- Power BI dashboard in `/dashboard/` for interactive visualizations.
- Excel tool:  `What-if analysis.xlsm` for analysis.
  
## Project structure
- `data/` : raw and processed data 
- `dashboard/` : PBI dashboard 
- `notebooks/` : Jupyter analysis notebooks
- `scripts/` : scripts for ETL and tests
- `config.yaml` : DB connection config 
- `stat_tests.py`: GUI entry point
- `queries.py`:  SQL queries for stat tests
- `fonts/`: optional local fonts
- `what-if analysis.xlsm`: excel analysis tool

## Installation
1. Clone repo: git clone https://github.com/drkrnman/Mobile-and-Website-Sales-Analysis.git
2. Install dependencies: pip install -r requirements.txt (assumes Python 3.9+).
3. Install ODBC Driver 17 for SQL Server.
   
## Database Setup (for GUI stats)
1. Install SQL Server Express (free).
2. Download `app.bak.zip` from https://drive.google.com/file/d/1JWYqbJVFPizV0kDJkmdG00ffLcTGwwAk/view?usp=drive_link, unzip.
3. Restore in DBeaver/sqlcmd:  
   `RESTORE DATABASE [app] FROM DISK = 'path\to\app.bak' WITH REPLACE;`
4. Config: `config.yaml` uses `localhost\\SQLEXPRESS` by default.

## Usage
- Notebooks: Open `/notebooks/` in Jupyter (run `etl.ipynb` and `eda.ipynb`).
- Power BI Dashboard: Open files in `/dashboard/` with Power BI Desktop.
- Excel tools: Open `What-if analysis.xlsm`.
- GUI stats tool : `python stat_tests.py`

## Data sourse link
https://www.kaggle.com/datasets/bytadit/transactional-ecommerce/data

## Authors
Darya Korenman
Ilia Usovich
