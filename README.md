# SQL Data Controller

A local Python desktop app for editing SQLite tables in a spreadsheet-like grid, with live CSV mirroring.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

```powershell
python main.py
```

The app creates runtime data under:

```text
data/database.sqlite
data/csv/<table_name>.csv
```

SQLite is the source of truth. CSV files are regenerated after successful table edits.

## Test

```powershell
python -m unittest discover
```

