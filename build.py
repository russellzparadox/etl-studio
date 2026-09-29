"""Build a standalone app for the current OS:  python build.py
PyInstaller cannot cross-compile; run this on each target OS (CI does it)."""
import PyInstaller.__main__ as pi

pi.run([
    "etl_studio/__main__.py",
    "--name", "ETLStudio",
    "--windowed",          # no console; .app bundle on macOS
    "--noconfirm", "--clean",
    "--collect-submodules", "sqlalchemy.dialects",
    "--hidden-import", "psycopg2",
    "--hidden-import", "pyodbc",
])
