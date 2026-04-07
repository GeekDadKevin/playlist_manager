param(
    [switch]$NoSync,
    [switch]$Production
)

$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

if (-not (Test-Path '.env') -and (Test-Path '.env.example')) {
    Copy-Item '.env.example' '.env'
    Write-Host 'Created .env from .env.example'
}

if (-not $NoSync) {
    uv sync --dev
}

$port = if ($env:APP_PORT) { $env:APP_PORT } else { '8000' }

if ($Production) {
    uv run waitress-serve --host=0.0.0.0 --port=$port app:app
}
else {
    uv run flask --app app run --debug --host 0.0.0.0 --port $port
}
