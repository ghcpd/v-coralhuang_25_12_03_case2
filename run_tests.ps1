param(
    [switch]$Concurrent
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$venv = Join-Path $root ".venv"
$python = Join-Path $venv "Scripts\python.exe"

if (!(Test-Path $python)) {
    Write-Host "Creating venv at $venv"
    python -m venv $venv
}

Write-Host "Installing requirements..."
& $python -m pip install --upgrade pip | Out-Null
& $python -m pip install -r (Join-Path $root "requirements.txt") | Out-Null

$env:OUTBOX_DB_URL = "sqlite+aiosqlite:///$root/outbox.db"

Write-Host "Seeding sample data..."
& $python -m outbox.test_harness --reset-db --seed 1000 --transient-fail 50 --dead-fail 20 | Write-Host

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

if ($Concurrent) {
    Write-Host "Running two forwarder instances concurrently..."
    $job1Args = @("--stop-when-idle","--batch-size","200","--concurrency","10","--lock-owner","inst1")
    $job2Args = @("--stop-when-idle","--batch-size","200","--concurrency","10","--lock-owner","inst2")
    $job1 = Start-Job -ScriptBlock { param($py,$args) & $py -m outbox.forwarder @args } -ArgumentList $python, $job1Args
    Start-Sleep -Seconds 1
    $job2 = Start-Job -ScriptBlock { param($py,$args) & $py -m outbox.forwarder @args } -ArgumentList $python, $job2Args
    Receive-Job $job1 -Wait | Write-Host
    Receive-Job $job2 -Wait | Write-Host
    Remove-Job $job1, $job2 -Force
} else {
    Write-Host "Running single forwarder instance..."
    & $python -m outbox.forwarder --stop-when-idle --batch-size 200 --concurrency 10
}

$stopwatch.Stop()

Write-Host "Collecting summary..."
$json = & $python -m outbox.test_harness --summary
$summary = $json | ConvertFrom-Json

Write-Host "==== Test Summary ===="
Write-Host ("Forwarder runtime (sec): {0:N2}" -f $stopwatch.Elapsed.TotalSeconds)
Write-Host ("Forwarded: {0}" -f $summary.forwarded)
Write-Host ("Failed: {0}" -f $summary.failed)
Write-Host ("DLQ: {0}" -f $summary.dlq)
Write-Host ("Lat avg (s): {0}" -f $summary.latency_avg_sec)
Write-Host ("Lat p95 (s): {0}" -f $summary.latency_p95_sec)
Write-Host "======================"
