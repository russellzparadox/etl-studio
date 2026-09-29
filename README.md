# ETL Studio
SQL Server <-> PostgreSQL migration GUI (PySide6).

## Run
    pip install -e .
    etl-studio          # or: python -m etl_studio

## Build a standalone app (for the OS you're on)
    pip install ".[build]"
    python build.py     # output in dist/

PyInstaller can't cross-compile: Windows/macOS/Linux builds come from
`.github/workflows/build.yml` (push a `v*` tag or run it manually).

SQL Server access needs the **Microsoft ODBC Driver 17/18** installed on the
machine running the app (it is not bundled).
