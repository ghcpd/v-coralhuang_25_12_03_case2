# Outbox Forwarder Test Runner
# This script sets up the environment, seeds data, runs the forwarder, and validates results.

param(
    [int]$SeedCount = 1000,
    [int]$TimeoutSeconds = 60,
    [switch]$MultiInstance
)

$ErrorActionPreference = "Stop"

Write-Host "=== Outbox Forwarder Test Runner ===" -ForegroundColor Cyan

# 1. Set up Python virtual environment
Write-Host "`n[1/7] Setting up Python virtual environment..." -ForegroundColor Yellow
if (-not (Test-Path "venv")) {
    Write-Host "Creating new virtual environment..."
    python -m venv venv
} else {
    Write-Host "Virtual environment already exists"
}

# Activate venv
$activateScript = ".\venv\Scripts\Activate.ps1"
if (Test-Path $activateScript) {
    & $activateScript
} else {
    Write-Host "Error: Could not find activation script" -ForegroundColor Red
    exit 1
}

# 2. Install dependencies
Write-Host "`n[2/7] Installing dependencies..." -ForegroundColor Yellow
python -m pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
Write-Host "Dependencies installed"

# 3. Clean up existing database
Write-Host "`n[3/7] Cleaning up existing database..." -ForegroundColor Yellow
if (Test-Path "outbox.db") {
    Remove-Item "outbox.db" -Force
    Write-Host "Removed existing database"
}

# 4. Seed test data
Write-Host "`n[4/7] Seeding $SeedCount outbox records..." -ForegroundColor Yellow
python -m outbox.test_utils seed $SeedCount
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error seeding data" -ForegroundColor Red
    exit 1
}

# Show initial stats
python -m outbox.test_utils stats

# 5. Run forwarder (single instance)
Write-Host "`n[5/7] Running forwarder (single instance)..." -ForegroundColor Yellow
$forwarderJob = Start-Job -ScriptBlock {
    param($scriptRoot)
    Set-Location $scriptRoot
    & ".\venv\Scripts\Activate.ps1"
    python -m outbox.forwarder
} -ArgumentList $PSScriptRoot

Write-Host "Forwarder started (Job ID: $($forwarderJob.Id))"

# Monitor progress
$startTime = Get-Date
$lastStats = $null

while (((Get-Date) - $startTime).TotalSeconds -lt $TimeoutSeconds) {
    Start-Sleep -Seconds 2
    
    # Check stats
    $statsOutput = python -m outbox.test_utils stats 2>&1 | Out-String
    
    # Parse stats
    if ($statsOutput -match "Outbox - New: (\d+)") {
        $newCount = [int]$matches[1]
    }
    if ($statsOutput -match "Outbox - Forwarded: (\d+)") {
        $forwardedCount = [int]$matches[1]
    }
    if ($statsOutput -match "Outbox - Failed: (\d+)") {
        $failedCount = [int]$matches[1]
    }
    if ($statsOutput -match "Outbox - Locked: (\d+)") {
        $lockedCount = [int]$matches[1]
    }
    
    $progress = [math]::Round(($forwardedCount / $SeedCount) * 100, 1)
    Write-Host "Progress: $forwardedCount/$SeedCount forwarded ($progress%) | New: $newCount | Failed: $failedCount | Locked: $lockedCount" -NoNewline
    Write-Host "`r" -NoNewline
    
    # Check if done
    if ($newCount -eq 0 -and $lockedCount -eq 0 -and $failedCount -eq 0) {
        Write-Host "`nAll records processed successfully!" -ForegroundColor Green
        break
    }
    
    if ($newCount -eq 0 -and $lockedCount -eq 0 -and $failedCount -gt 0) {
        Write-Host "`nProcessing complete with some failures" -ForegroundColor Yellow
        break
    }
}

Write-Host ""

# Stop forwarder
Write-Host "Stopping forwarder..." -ForegroundColor Yellow
Stop-Job -Job $forwarderJob -ErrorAction SilentlyContinue
Remove-Job -Job $forwarderJob -ErrorAction SilentlyContinue

# 6. Optional: Multi-instance test
if ($MultiInstance) {
    Write-Host "`n[6/7] Testing multi-instance locking..." -ForegroundColor Yellow
    
    # Seed more data
    python -m outbox.test_utils seed 500
    
    # Start two forwarder instances
    $job1 = Start-Job -ScriptBlock {
        param($scriptRoot)
        Set-Location $scriptRoot
        & ".\venv\Scripts\Activate.ps1"
        $env:OUTBOX_SCAN_INTERVAL = "0.5"
        python -m outbox.forwarder
    } -ArgumentList $PSScriptRoot
    
    $job2 = Start-Job -ScriptBlock {
        param($scriptRoot)
        Set-Location $scriptRoot
        & ".\venv\Scripts\Activate.ps1"
        $env:OUTBOX_SCAN_INTERVAL = "0.5"
        python -m outbox.forwarder
    } -ArgumentList $PSScriptRoot
    
    Write-Host "Started 2 forwarder instances (Job IDs: $($job1.Id), $($job2.Id))"
    Start-Sleep -Seconds 10
    
    Stop-Job -Job $job1, $job2 -ErrorAction SilentlyContinue
    Remove-Job -Job $job1, $job2 -ErrorAction SilentlyContinue
    
    Write-Host "Multi-instance test complete"
} else {
    Write-Host "`n[6/7] Skipping multi-instance test (use -MultiInstance flag to enable)" -ForegroundColor Gray
}

# 7. Final statistics and validation
Write-Host "`n[7/7] Final Results:" -ForegroundColor Yellow
python -m outbox.test_utils stats

# Calculate metrics
$finalStatsOutput = python -m outbox.test_utils stats 2>&1 | Out-String

if ($finalStatsOutput -match "Outbox - Forwarded: (\d+)") {
    $finalForwarded = [int]$matches[1]
}
if ($finalStatsOutput -match "Events Table: (\d+)") {
    $finalEvents = [int]$matches[1]
}
if ($finalStatsOutput -match "DLQ: (\d+)") {
    $finalDLQ = [int]$matches[1]
}
if ($finalStatsOutput -match "Audit Entries: (\d+)") {
    $finalAudit = [int]$matches[1]
}

$elapsedSeconds = ((Get-Date) - $startTime).TotalSeconds
$avgLatency = if ($finalForwarded -gt 0) { $elapsedSeconds / $finalForwarded } else { 0 }

Write-Host "`n=== Test Summary ===" -ForegroundColor Cyan
Write-Host "Total Seeded: $SeedCount"
Write-Host "Forwarded: $finalForwarded"
Write-Host "Events Created: $finalEvents"
Write-Host "Dead Letters: $finalDLQ"
Write-Host "Audit Entries: $finalAudit"
Write-Host "Total Time: $([math]::Round($elapsedSeconds, 2))s"
Write-Host "Avg Latency: $([math]::Round($avgLatency * 1000, 2))ms per record"

# Validation
$success = $true
if ($finalForwarded -lt ($SeedCount * 0.95)) {
    Write-Host "`nWARNING: Less than 95% of records forwarded" -ForegroundColor Yellow
    $success = $false
}
if ($finalEvents -ne $finalForwarded) {
    Write-Host "`nWARNING: Event count mismatch (Events: $finalEvents, Forwarded: $finalForwarded)" -ForegroundColor Yellow
    $success = $false
}
if ($avgLatency -gt 3) {
    Write-Host "`nWARNING: Average latency exceeds 3s per record" -ForegroundColor Yellow
    $success = $false
}

if ($success) {
    Write-Host "`n✓ All acceptance criteria met!" -ForegroundColor Green
} else {
    Write-Host "`n✗ Some acceptance criteria not met" -ForegroundColor Red
}

Write-Host "`n=== Test Complete ===" -ForegroundColor Cyan
