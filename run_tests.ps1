# One-click test runner for Outbox Forwarder
# Sets up venv, installs deps, seeds data, runs forwarder, prints summary

param(
    [int]$SeedCount = 500,
    [float]$FailureRate = 0.02,
    [int]$ForwarderDurationSeconds = 10,
    [switch]$DualInstance = $false
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

# Paths
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvPath = Join-Path $ScriptDir "venv"
$PythonExe = Join-Path $VenvPath "Scripts\python.exe"
$RequirementsFile = Join-Path $ScriptDir "requirements.txt"

Write-Host "================================================" -ForegroundColor Green
Write-Host "Outbox Forwarder Test Runner" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green

# Step 1: Setup venv if missing
if (-not (Test-Path $VenvPath)) {
    Write-Host "`n[1/5] Creating Python venv..." -ForegroundColor Cyan
    python -m venv $VenvPath
    if ($LASTEXITCODE -ne 0) { throw "Failed to create venv" }
} else {
    Write-Host "`n[1/5] Using existing venv at $VenvPath" -ForegroundColor Cyan
}

# Step 2: Install requirements
Write-Host "`n[2/5] Installing requirements..." -ForegroundColor Cyan
& $PythonExe -m pip install -q -r $RequirementsFile
if ($LASTEXITCODE -ne 0) { throw "Failed to install requirements" }
Write-Host "  ✓ Requirements installed" -ForegroundColor Green

# Step 3: Cleanup old database and seed new data
Write-Host "`n[3/5] Seeding test data ($SeedCount records, $($FailureRate * 100)% failure rate)..." -ForegroundColor Cyan
$null = & $PythonExe -m outbox.test_harness cleanup
Start-Sleep -Milliseconds 500
$null = & $PythonExe -m outbox.test_harness seed $SeedCount $FailureRate
Start-Sleep -Milliseconds 500
Write-Host "  ✓ Test data seeded" -ForegroundColor Green

# Step 4: Run forwarder (single instance or dual instance)
Write-Host "`n[4/5] Running forwarder..." -ForegroundColor Cyan

$StartTime = Get-Date

if ($DualInstance) {
    Write-Host "  Starting dual-instance test (may show no new records if first instance processes all)..." -ForegroundColor Yellow
    
    # Run forwarder for instance 1
    Write-Host "  Instance 1: Running for $ForwarderDurationSeconds seconds..." -ForegroundColor Yellow
    & $PythonExe -c @"
import asyncio
from outbox.forwarder import run_loop

async def main_with_timeout():
    try:
        await asyncio.wait_for(run_loop(), timeout=$ForwarderDurationSeconds)
    except asyncio.TimeoutError:
        pass

try:
    asyncio.run(main_with_timeout())
except KeyboardInterrupt:
    pass
"@ | Out-Null
    
    Start-Sleep -Seconds 1
    
    # Run forwarder for instance 2
    Write-Host "  Instance 2: Running for $ForwarderDurationSeconds seconds..." -ForegroundColor Yellow
    & $PythonExe -c @"
import asyncio
from outbox.forwarder import run_loop

async def main_with_timeout():
    try:
        await asyncio.wait_for(run_loop(), timeout=$ForwarderDurationSeconds)
    except asyncio.TimeoutError:
        pass

try:
    asyncio.run(main_with_timeout())
except KeyboardInterrupt:
    pass
"@ | Out-Null
    
    Write-Host "  ✓ Dual-instance test completed" -ForegroundColor Green
} else {
    # Single instance
    Write-Host "  Running for $ForwarderDurationSeconds seconds..." -ForegroundColor Yellow
    & $PythonExe -c @"
import asyncio
from outbox.forwarder import run_loop

async def main_with_timeout():
    try:
        await asyncio.wait_for(run_loop(), timeout=$ForwarderDurationSeconds)
    except asyncio.TimeoutError:
        pass

try:
    asyncio.run(main_with_timeout())
except KeyboardInterrupt:
    pass
"@ | Out-Null
    
    Write-Host "  ✓ Forwarder completed" -ForegroundColor Green
}

$Duration = ((Get-Date) - $StartTime).TotalSeconds

# Step 5: Print summary
Write-Host "`n[5/5] Summary:" -ForegroundColor Cyan
$Summary = & $PythonExe -m outbox.test_harness stats
Write-Host $Summary

Write-Host "Forwarder execution time: $([Math]::Round($Duration, 1))s" -ForegroundColor Cyan
Write-Host "`n================================================" -ForegroundColor Green
Write-Host "Test Complete" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
