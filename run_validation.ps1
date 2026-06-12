<#
  run_validation.ps1  (.env-driven, one-line run)
  Runs 3 scenarios x N repeats. Each run: clean index=main -> generate data ->
  wait for indexing -> run agent diagnosis -> collect the report.

  All secrets (API key, HEC token, Splunk host/user/pass) are read from .env in
  this folder. You do NOT pass them on the command line.

  ── ONE-TIME PREREQUISITE ────────────────────────────────────
  Enable can_delete for your Splunk user (Settings -> Access Controls -> Users
  -> your user -> add the can_delete role -> Save). Without it the clean step fails.

  ── .env MUST CONTAIN (already set up) ───────────────────────
    ANTHROPIC_API_KEY=...
    PIPELINE_HEC_TOKEN=...
    SPLUNK_HEC_HOST=localhost
    SPLUNK_HOST=https://localhost:8089
    SPLUNK_USERNAME=...
    SPLUNK_PASSWORD=...        <-- must be filled (blank would make agent.py hang waiting for input)

  ── USAGE ────────────────────────────────────────────────────
    .\run_validation.ps1
  or with options:
    .\run_validation.ps1 -Repeats 5 -IndexWaitSec 12
#>

param(
    [int]$Repeats = 5,
    [int]$IndexWaitSec = 12   # indexing lag after generating; increase if queries miss data
)

$ErrorActionPreference = "Stop"

# Make Python emit UTF-8 so emoji output doesn't crash on Windows PowerShell.
$env:PYTHONUTF8 = "1"
try {
    $OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
} catch { }

# ── Parse .env and load every key into this process's environment ──
if (-not (Test-Path ".env")) {
    Write-Host "ERROR: .env not found in this folder. Run from the project root." -ForegroundColor Red
    exit 1
}
$envVars = @{}
Get-Content ".env" | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
        $idx = $line.IndexOf("=")
        $k = $line.Substring(0, $idx).Trim()
        $v = $line.Substring($idx + 1).Trim()
        $envVars[$k] = $v
        Set-Item -Path ("Env:" + $k) -Value $v   # child python procs inherit this
    }
}

$SplunkHost = $envVars["SPLUNK_HOST"]
$SplunkUser = $envVars["SPLUNK_USERNAME"]
$SplunkPass = $envVars["SPLUNK_PASSWORD"]

if ([string]::IsNullOrEmpty($SplunkPass)) {
    Write-Host "ERROR: SPLUNK_PASSWORD is blank in .env. Fill it in (the batch can't answer an interactive password prompt)." -ForegroundColor Red
    exit 1
}

$scenarios = @("schema_change", "volume_drop", "freshness_delay")
$outDir = Join-Path (Get-Location) "validation_runs"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$scoresCsv = Join-Path $outDir "scores.csv"
if (-not (Test-Path $scoresCsv)) {
    "scenario,run,report_file,level_TODO,notes_TODO" | Out-File -FilePath $scoresCsv -Encoding utf8
}

# ── Self-signed cert: let PS 5.1 accept localhost's self-signed HTTPS ──
try {
    Add-Type @"
using System.Net;
using System.Security.Cryptography.X509Certificates;
public class TrustAllCertsPolicy : ICertificatePolicy {
    public bool CheckValidationResult(ServicePoint sp, X509Certificate cert, WebRequest req, int problem) { return true; }
}
"@ -ErrorAction SilentlyContinue
    [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocol]::Tls12
} catch { }

$pair = "{0}:{1}" -f $SplunkUser, $SplunkPass
$basic = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
$splunkHeaders = @{ Authorization = "Basic $basic" }

function Clear-Index {
    Write-Host "  [clean] cleaning index=main ..." -ForegroundColor Yellow
    try {
        $body = @{ search = "search index=main | delete"; exec_mode = "oneshot"; output_mode = "json" }
        Invoke-RestMethod -Method Post -Uri "$SplunkHost/services/search/jobs" `
            -Headers $splunkHeaders -Body $body -TimeoutSec 120 | Out-Null
        Start-Sleep -Seconds 3
        Write-Host "  [clean] done" -ForegroundColor Green
    } catch {
        Write-Host "  [clean] FAILED (likely can_delete not enabled): $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "  [clean] continuing, but this run's data may be mixed with the previous run." -ForegroundColor Red
    }
}

$total = $scenarios.Count * $Repeats
$i = 0
$startAll = Get-Date

Write-Host "Config: host=$SplunkHost user=$SplunkUser repeats=$Repeats (secrets loaded from .env)" -ForegroundColor DarkGray

foreach ($s in $scenarios) {
    for ($run = 1; $run -le $Repeats; $run++) {
        $i++
        Write-Host ""
        Write-Host "==================================================================" -ForegroundColor Cyan
        Write-Host (" [{0}/{1}] scenario={2}  run={3}" -f $i, $total, $s, $run) -ForegroundColor Cyan
        Write-Host "==================================================================" -ForegroundColor Cyan

        Clear-Index

        Write-Host "  [gen] generating $s data ..." -ForegroundColor Yellow
        python generate_data.py --scenario $s | Out-Null

        Write-Host "  [wait] waiting $IndexWaitSec sec for Splunk indexing ..." -ForegroundColor Yellow
        Start-Sleep -Seconds $IndexWaitSec

        $reportFile = Join-Path $outDir ("{0}_run{1}.txt" -f $s, $run)
        Write-Host "  [agent] diagnosing, output -> $reportFile" -ForegroundColor Yellow
        python agent.py --scenario $s --verbose *> $reportFile

        ('{0},{1},{2},,' -f $s, $run, ("validation_runs\{0}_run{1}.txt" -f $s, $run)) |
            Out-File -FilePath $scoresCsv -Encoding utf8 -Append

        Write-Host "  [done] run complete" -ForegroundColor Green
        Start-Sleep -Seconds 5
    }
}

$elapsed = (Get-Date) - $startAll
Write-Host ""
Write-Host "All done: $total runs, took $([int]$elapsed.TotalMinutes) minutes." -ForegroundColor Green
Write-Host "Reports in: $outDir" -ForegroundColor Green
Write-Host "Scoring sheet: $scoresCsv (fill level and notes per L4_rubric.md)" -ForegroundColor Green
