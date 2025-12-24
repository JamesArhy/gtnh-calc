param(
  [switch]$Install
)

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$env:GTNH_INSTALL = if ($Install) { "1" } else { "0" }

function Select-BackendPort {
  $preferred = 8000
  try {
    $listeners = @(Get-NetTCPConnection -LocalPort $preferred -State Listen -ErrorAction SilentlyContinue)
    $wslRelay = $false
    foreach ($listener in $listeners) {
      if ($listener.LocalAddress -in @("127.0.0.1", "::1")) {
        try {
          $proc = Get-Process -Id $listener.OwningProcess -ErrorAction Stop
          if ($proc.ProcessName -eq "wslrelay") {
            $wslRelay = $true
            break
          }
        } catch {
        }
      }
    }
    if ($wslRelay) {
      return 8001
    }
    if (-not (Test-NetConnection -ComputerName "127.0.0.1" -Port $preferred -InformationLevel Quiet)) {
      return $preferred
    }
    $response = Invoke-WebRequest -Uri "http://localhost:$preferred/api/versions" -UseBasicParsing -TimeoutSec 1 -ErrorAction Stop
    if ($response.StatusCode -eq 200) {
      return $preferred
    }
  } catch {
    return 8001
  }
  return 8001
}

$backendPort = Select-BackendPort
Write-Host "Using backend port $backendPort"
$frontendEnvPath = Join-Path $repoRoot "frontend\\.env.local"
"VITE_API_BASE=http://127.0.0.1:$backendPort" | Set-Content -Path $frontendEnvPath -Encoding ASCII

function Start-Backend {
  $backendDir = Join-Path $repoRoot "backend"
  $backendCmd = @"
`$Install = `$env:GTNH_INSTALL -eq '1'
`$env:GTNH_API_PORT = "$backendPort"
if (Test-Path .venv\\Scripts\\Activate.ps1) {
  . .venv\\Scripts\\Activate.ps1
}
if (`$Install) {
  python -m pip install -r requirements.txt
}
python -m app.main
"@
  Start-Process powershell -WorkingDirectory $backendDir -ArgumentList "-NoExit", "-Command", $backendCmd
}

function Start-Frontend {
  $frontendDir = Join-Path $repoRoot "frontend"
  $frontendCmd = @"
`$Install = `$env:GTNH_INSTALL -eq '1'
`$env:VITE_API_BASE = "http://localhost:$backendPort"
if (`$Install -or -not (Test-Path node_modules)) {
  npm install
}
npm run dev
"@
  Start-Process powershell -WorkingDirectory $frontendDir -ArgumentList "-NoExit", "-Command", $frontendCmd
}

function Wait-Backend {
  param(
    [int]$Port = 8000,
    [int]$Retries = 60,
    [int]$DelaySeconds = 1
  )
  for ($i = 0; $i -lt $Retries; $i++) {
    try {
      $client = New-Object System.Net.Sockets.TcpClient
      $client.Connect("127.0.0.1", $Port)
      if ($client.Connected) {
        $client.Close()
        return $true
      }
    } catch {
      if ($client) {
        $client.Close()
      }
      Start-Sleep -Seconds $DelaySeconds
    }
  }
  return $false
}

Start-Backend
Wait-Backend -Port $backendPort | Out-Null
Start-Sleep -Seconds 2
Start-Frontend
