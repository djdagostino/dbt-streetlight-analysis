# run.ps1 — run the streetlight pipeline from Windows.
#
#   .\run.ps1                    full pipeline: sync WGE -> raw.*, then dbt build
#   .\run.ps1 debug              -> dbt debug   (validate the warehouse connection)
#   .\run.ps1 run -s some_model  -> dbt run -s some_model
#   .\run.ps1 build --full-refresh
#
# Thin launcher: it just hands off to run.py using the project venv's Python.
# No dot-sourcing, no activation — run.py loads .env and configures dbt itself.

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    Write-Error ("No virtualenv found at $python`n" +
                 "Create it once:  python -m venv .venv`n" +
                 "                 .\.venv\Scripts\pip install -r requirements.txt")
    exit 1
}

& $python (Join-Path $PSScriptRoot "run.py") @args
exit $LASTEXITCODE
