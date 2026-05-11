# Activate.ps1 - bootstrap a PowerShell session for this dbt project.
#
# Usage:   . .\Activate.ps1
#          (note the leading dot - dot-source so env changes apply to your shell)
#
# Does three things:
#   1. Activates the Python venv at .venv\
#   2. Loads .env into the current process environment (Python + dbt both read it)
#   3. Points dbt at the dbt\ subdirectory via DBT_PROJECT_DIR + DBT_PROFILES_DIR,
#      so `dbt run` etc. work from anywhere in the repo (no need to cd dbt\)

$ErrorActionPreference = "Stop"

# --- 1. venv ---------------------------------------------------------------
$venvActivate = Join-Path $PSScriptRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $venvActivate) {
    & $venvActivate
    Write-Host "venv activated"
} else {
    Write-Host "WARNING: no .venv found at $venvActivate" -ForegroundColor Yellow
}

# --- 2. .env ---------------------------------------------------------------
$envFile = Join-Path $PSScriptRoot ".env"
if (Test-Path $envFile) {
    $loaded = 0
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^=#\s][^=]*)=(.*)\s*$') {
            $name  = $matches[1].Trim()
            $value = $matches[2].Trim().Trim('"').Trim("'")
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
            $loaded++
        }
    }
    Write-Host ".env loaded ($loaded variables)"
} else {
    Write-Host "WARNING: no .env found at $envFile" -ForegroundColor Yellow
    Write-Host "         copy .env.example to .env and fill in values"
}

# --- 3. dbt project + profiles location -----------------------------------
$dbtDir = Join-Path $PSScriptRoot "dbt"
if (Test-Path $dbtDir) {
    $env:DBT_PROJECT_DIR  = $dbtDir
    $env:DBT_PROFILES_DIR = $dbtDir
    Write-Host "DBT_PROJECT_DIR  = $dbtDir"
    Write-Host "DBT_PROFILES_DIR = $dbtDir"
} else {
    Write-Host "WARNING: no dbt\ directory found at $dbtDir" -ForegroundColor Yellow
}
