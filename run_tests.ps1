$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path ".venv")) {
  python -m venv .venv
}
. .\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt

Write-Host "Seeding, running forwarder, and summarizing..." -ForegroundColor Cyan
python -m outbox.test_runner --num 1000 --fail-ratio 0.02 --instances 2
