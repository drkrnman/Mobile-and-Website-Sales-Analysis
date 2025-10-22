# Mobile app and Website Sales Analysis

A project for analyzing sales through a mobile application and website.

## Project structure

- `data/` - raw and processed data 
- `dashboard/` - dashboard 
- `notebooks/` — Jupyter notebooks
- `scripts/` — scripts for ETL and tests
- `config.yaml` - DB connection config 
- `stat_tests.py`: GUI entry point
- `queries.py`:  SQL queries for stat tests
- `fonts/`: optional local fonts

### Requirements
- Python 3.9+
- A working DB reachable by SQLAlchemy URL defined in `config.yaml` (via `stattest.py`)

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
