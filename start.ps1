param(
    [switch]$NoSync,
    [switch]$Production
)

$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$lockFile = Join-Path $PSScriptRoot '.app.lock'

if (-not (Test-Path '.env') -and (Test-Path '.env.example')) {
    Copy-Item '.env.example' '.env'
    Write-Host 'Created .env from .env.example'
}

if (Test-Path '.env') {
    Get-Content '.env' | ForEach-Object {
        if ($_ -notmatch '^\s*(#|$)') {
            $parts = $_ -split '=', 2
            if ($parts.Count -eq 2) {
                [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), 'Process')
            }
        }
    }
}

if (-not $NoSync) {
    uv sync --dev
}

$pythonExe = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found at $pythonExe. Run uv sync --dev first."
}

& $pythonExe (Join-Path $PSScriptRoot 'validate_env.py')
if ($LASTEXITCODE -ne 0) {
    throw "Environment validation failed. Fix the .env values listed above and retry."
}

if (Test-Path $lockFile) {
    $existingPidText = (Get-Content $lockFile -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
    if ($existingPidText -match '^\d+$') {
        $existingProcess = Get-Process -Id ([int]$existingPidText) -ErrorAction SilentlyContinue
        if ($existingProcess) {
            Write-Host "Stopping previous Playlist Sync process (PID $existingPidText)..."
            Stop-Process -Id $existingProcess.Id -Force
            Wait-Process -Id $existingProcess.Id -Timeout 5 -ErrorAction SilentlyContinue
        }
    }
    Remove-Item $lockFile -ErrorAction SilentlyContinue
}

$repoAppPids = Get-CimInstance Win32_Process |
    Where-Object {
        $_.CommandLine -and
        $_.CommandLine -like "*$PSScriptRoot*" -and
        $_.CommandLine -match 'flask|waitress'
    } |
    Select-Object -ExpandProperty ProcessId -Unique
foreach ($procId in $repoAppPids) {
    if ($procId) {
        Write-Host "Stopping stale repo app process (PID $procId)..."
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
        Wait-Process -Id $procId -Timeout 5 -ErrorAction SilentlyContinue
    }
}

$port = 3000
[Environment]::SetEnvironmentVariable('APP_PORT', "$port", 'Process')

$portListeners = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique
foreach ($procId in $portListeners) {
    if ($procId) {
        Write-Host "Clearing process on port $port (PID $procId)..."
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
        Wait-Process -Id $procId -Timeout 5 -ErrorAction SilentlyContinue
    }
}

$portCleared = $false
for ($attempt = 0; $attempt -lt 10; $attempt++) {
    if (-not (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)) {
        $portCleared = $true
        break
    }
    Start-Sleep -Milliseconds 500
}

if (-not $portCleared) {
    throw "Port $port is still in use after cleanup. Stop the conflicting process and retry."
}

Write-Host "Starting Playlist Sync UI + API on http://127.0.0.1:$port"

$env:PYTHONPATH = if ($env:PYTHONPATH) { "$PSScriptRoot;$($env:PYTHONPATH)" } else { $PSScriptRoot }

$args = if ($Production) {
    @('-m', 'waitress', '--host=0.0.0.0', "--port=$port", 'app:app')
}
else {
    @('-m', 'flask', '--app', 'app:create_app', 'run', '--debug', '--no-reload', '--host', '0.0.0.0', '--port', "$port")
}

$process = Start-Process -FilePath $pythonExe -ArgumentList $args -WorkingDirectory $PSScriptRoot -PassThru
Set-Content -Path $lockFile -Value $process.Id
Write-Host "Playlist Sync started with PID $($process.Id). Lock file: $lockFile"
Write-Host "Open http://127.0.0.1:$port/"
