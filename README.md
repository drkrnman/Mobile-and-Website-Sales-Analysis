# Mobile app and Website Sales Analysis

A project for analyzing sales through a mobile application and website.

## Project structure

- `data/` - raw and processed data 
- `dashboard/` - PBI dashboard 
- `notebooks/` — Jupyter analysis notebooks
- `scripts/` — SQL-scripts for ETL and tests
- `config.yaml` - DB connection config 
- `stat_tests.py`: GUI entry point
- `queries.py`:  SQL queries for stat tests
- `fonts/`: optional local fonts
- `what-if analysis.xlsm`: excel analysis tool
## Installation
1. Clone repo: git clone https://github.com/drkrnman/Mobile-and-Website-Sales-Analysis.git
2. Install dependencies: pip install -r requirements.txt (assumes Python 3.9+).
3. Install ODBC Driver 17 for SQL Server.
4. 
## Database Setup (for GUI stats)
1. Install SQL Server Express (free).
2. Download `app.bak.zip` from Releases, unzip.
3. Restore in DBeaver/sqlcmd:  
   `RESTORE DATABASE [app] FROM DISK = 'path\to\app.bak' WITH REPLACE;`
4. Config: `config.yaml` uses `localhost\\SQLEXPRESS` by default.
Install deps:
```bash
pip install -r requirements.txt
```

### Database configuration
The GUI reuses the SQLAlchemy engine from `stattest.py` which reads `config.yaml`. Example:
```yaml
db_url: mssql+pyodbc://username:password@localhost/yourdb?driver=ODBC+Driver+17+for+SQL+Server
```


### Run
```bash
!!!!!!!!python stat_tests.py
```

## Statistical Tests GUI

A Tkinter GUI to run Student's t-tests and Chi-square tests against your database with ready-made metrics and grouping options.

### Features

### Notes
-
- If Montserrat is not found and `tkextrafont` not installed, the app falls back to a default system font.
