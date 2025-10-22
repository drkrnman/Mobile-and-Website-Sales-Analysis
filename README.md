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
Ensure all referenced tables exist:
- `rd_transactions`, `rd_customers`, `rd_transactions_prods`, `rd_sessions`, `rd_prods`
- For Chi-square, a fallback query uses `dm_transactions` if present.

### Run
```bash
python stat_tests.py
```
### Fonts (optional but recommended)
Create `fonts/` directory next to `stat_tests.py` and place:
- `Montserrat-Regular.ttf`
- `Montserrat-MediumItalic.ttf` (optional)
- `Montserrat-Bold.ttf` (optional)

To allow loading fonts without installing system-wide:
```bash
pip install tkextrafont
```
The app will auto-load fonts from `fonts/` if present.

## Statistical Tests GUI

A Tkinter GUI to run Student's t-tests and Chi-square tests against your database with ready-made metrics and grouping options.

### Features
- t-test metrics: Average order value, Number of items per order, Number of clicks before booking, Average delivery cost, Number of unique products
- Grouping: Male vs Female or WEB vs MOBILE
- Chi-square test: payment_method distribution across selected grouping (counts + column % + decision)
- Montserrat fonts (optional local load)
- Accent-colored controls and scrollbars
- Back/Exit navigation
- Modal loading indicator with indeterminate progress during test runs

### Notes
- If the loader window does not appear on your OS/WM, it still runs tests; report your OS/DE so we can tweak window flags.
- If Montserrat is not found and `tkextrafont` not installed, the app falls back to a default system font.
