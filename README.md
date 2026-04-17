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

On first launch, the app creates runtime data under:

```text
data/database.sqlite
data/csv/<table_name>.csv
```

When you open or create another database, its CSV mirror is stored beside it:

```text
example.sqlite
example_csv/<table_name>.csv
```

SQLite is the source of truth. CSV files are regenerated after successful table edits.

## Features

- Open or create SQLite database files.
- Create, rename, rebuild, and delete tables.
- Add, rename, delete, drag, and multi-select rows/columns.
- Edit and empty selected cells.
- Search with highlight or row filtering.
- Switch CSV preview between raw highlighted text and read-only table mode.
- View the persistent change log and use common undo/redo.

## Test

```powershell
python -m unittest discover
```
