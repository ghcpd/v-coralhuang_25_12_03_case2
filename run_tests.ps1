<#
One-click test runner for this folder.

This script will:
 - create a Python venv in .venv if missing
 - install the local requirements.txt
 - seed the outbox DB and run the forwarder
#>
param(
    [int]$Seed = 500,
    [switch]$SpawnSecond
)

if (-not (Test-Path -Path .venv)) {
    python -m venv .venv; Write-Host "Created venv"
}

& .\.venv\Scripts\pip.exe install --upgrade pip
& .\.venv\Scripts\pip.exe install -r requirements.txt

Write-Host "Seeding database with $Seed rows..."
& .\.venv\Scripts\python.exe -c "from outbox.scripts import run_tests; import asyncio; asyncio.run(run_tests(seed_count=$($Seed), spawn_second_instance=$($SpawnSecond)))"
