<#  run-beam-docker-test.ps1
    Smoke test: Beam JobServer + Spark with DOCKER environment on Windows PowerShell

    Usage:
      .\run-beam-docker-test.ps1
      .\run-beam-docker-test.ps1 -SdkImage "apache/beam_python3.10_sdk:2.59.0" -PrePull:$false
#>

param(
  [string]$ProjectRoot = (Get-Location).Path,
  [string]$InputLocal  = "$PWD\data\words.txt",
  [string]$OutputLocal = "$PWD\data",
  [string]$SdkImage    = "apache/beam_python3.10_sdk:2.59.0",
  [string]$JobEndpoint = "beam-jobserver:8099",
  [string]$ArtifactEndpoint = "beam-jobserver:8098",
  [switch]$PrePull = $true
)

$ErrorActionPreference = "Stop"

Write-Host "==> Validating Docker..." -ForegroundColor Cyan
docker version | Out-Null

# Compose default network name: <folder>_default
$proj = Split-Path -Leaf $ProjectRoot
$net  = "${proj}_default"
Write-Host "==> Using compose network: $net" -ForegroundColor Cyan

# Ensure the network exists
$netExists = (docker network ls --format "{{.Name}}" | Select-String -SimpleMatch $net) -ne $null
if (-not $netExists) {
  throw "Docker network '$net' not found. Did you run 'docker compose up -d' in this folder?"
}

# Ensure data dir + input file
if (-not (Test-Path "$PWD\data")) { New-Item -ItemType Directory -Path "$PWD\data" | Out-Null }
if (-not (Test-Path $InputLocal)) {
  Write-Host "==> Creating sample input at $InputLocal" -ForegroundColor Yellow
  "hello beam hello spark" | Set-Content -NoNewline -Encoding utf8 $InputLocal
}

# Optional: pre-pull SDK image (the worker will also pull via docker.sock)
if ($PrePull) {
  Write-Host "==> Pre-pulling SDK image: $SdkImage" -ForegroundColor Cyan
  docker pull $SdkImage | Out-Null
}

# Build JSON for --environment_config safely
$envCfg = @{ container_image = $SdkImage } | ConvertTo-Json -Compress

# Show quick reachability from host (ports are published in your compose)
Write-Host "==> Checking JobServer ports on host (8099/8098)..." -ForegroundColor Cyan
try {
  Test-NetConnection -ComputerName localhost -Port 8099 -WarningAction SilentlyContinue | Out-Null
  Test-NetConnection -ComputerName localhost -Port 8098 -WarningAction SilentlyContinue | Out-Null
} catch { Write-Host "   (Host check skipped; continuing)" -ForegroundColor DarkYellow }

# Run the job (client container joins the compose network)
Write-Host "==> Submitting Beam wordcount job via DOCKER environment..." -ForegroundColor Cyan
$runArgs = @(
  "--rm","-i",
  "--network", $net,
  "-v", ("{0}:/data" -f (Join-Path $PWD "data")),
  $SdkImage,
  "python","-m","apache_beam.examples.wordcount",
  "--input","/data/words.txt",
  "--output","/data/out",
  "--runner","PortableRunner",
  "--job_endpoint", $JobEndpoint,
  "--artifact_endpoint", $ArtifactEndpoint,
  "--environment_type","DOCKER",
  "--environment_config", $envCfg
)

# Use 'docker run -t' only if interactive console; otherwise skip -t
if ($Host.UI.RawUI.KeyAvailable -or $env:WT_SESSION) {
  $runArgs = @("-t") + $runArgs
}

# Execute
$proc = Start-Process -FilePath "docker" -ArgumentList @("run") + $runArgs -NoNewWindow -PassThru -Wait
if ($proc.ExitCode -ne 0) {
  throw "Beam job failed (exit code $($proc.ExitCode)). Check 'spark-worker' logs and docker permissions for /var/run/docker.sock."
}

# Show results
Write-Host "==> Job finished. Output files:" -ForegroundColor Green
Get-ChildItem $OutputLocal | Where-Object { $_.Name -like "out*" } | ForEach-Object {
  Write-Host (" - " + $_.FullName)
}

$outs = Get-ChildItem $OutputLocal | Where-Object { $_.Name -like "out*" }
if ($outs) {
  Write-Host "`n==> Sample output:" -ForegroundColor Green
  foreach ($f in $outs) { Get-Content $f }
} else {
  Write-Host "No output files found under $OutputLocal. Did the pipeline write to /data/out*?" -ForegroundColor Yellow
}

Write-Host "`n✅ Beam DOCKER environment smoke test succeeded." -ForegroundColor Green
