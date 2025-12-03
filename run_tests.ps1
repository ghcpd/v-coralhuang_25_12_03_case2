Param(
    [switch]$Seed,
    [switch]$SecondInstance,
    [int]$Wait = 20
)

$venvPath = Join-Path $PSScriptRoot ".venv"
if (!(Test-Path $venvPath)) {
    python -m venv $venvPath
}

$activate = Join-Path $venvPath "Scripts\Activate.ps1"
. $activate

pip install -r ./outbox/requirements.txt

$seedArg = if ($Seed) { '--seed' } else { '' }
$secondArg = if ($SecondInstance) { '--second' } else { '' }

python -m outbox.run_tests $seedArg $secondArg --wait $Wait

Write-Output "Test run complete"