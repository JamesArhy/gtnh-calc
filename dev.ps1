param(
  [switch]$Install
)

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$env:GTNH_INSTALL = if ($Install) { "1" } else { "0" }

function Start-Backend {
  $backendDir = Join-Path $repoRoot "backend"
  $backendCmd = @"
cd '$backendDir'
`$Install = `$env:GTNH_INSTALL -eq '1'
if (Test-Path .venv\\Scripts\\Activate.ps1) {
  . .venv\\Scripts\\Activate.ps1
}
if (`$Install) {
  python -m pip install -r requirements.txt
}
python -m app.main
"@
  Start-Process powershell -ArgumentList "-NoExit", "-Command", $backendCmd
}

function Start-Frontend {
  $frontendDir = Join-Path $repoRoot "frontend"
  $frontendCmd = @"
cd '$frontendDir'
`$Install = `$env:GTNH_INSTALL -eq '1'
if (`$Install -or -not (Test-Path node_modules)) {
  npm install
}
npm run dev
"@
  Start-Process powershell -ArgumentList "-NoExit", "-Command", $frontendCmd
}

function Wait-Backend {
  param(
    [int]$Retries = 60,
    [int]$DelaySeconds = 1
  )
  for ($i = 0; $i -lt $Retries; $i++) {
    try {
      Invoke-WebRequest -Uri "http://localhost:8000/api/versions" -UseBasicParsing | Out-Null
      return $true
    } catch {
      Start-Sleep -Seconds $DelaySeconds
    }
  }
  return $false
}

Start-Backend
Wait-Backend | Out-Null
Start-Sleep -Seconds 2
Start-Frontend
