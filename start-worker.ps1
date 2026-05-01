# Boots the AI editing worker for local testing.
#
# Usage:  .\start-worker.ps1
# Stop:   Ctrl+C
#
# If PowerShell blocks the script with an execution-policy error, run this
# once in any PowerShell window and you'll never see it again:
#
#   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
#
# Or invoke the script with bypass for a single run:
#
#   powershell -ExecutionPolicy Bypass -File .\start-worker.ps1

# nvm-windows path setup — guarantees `node` and `npm` resolve regardless
# of how this shell was launched. Safe to leave in place even when the
# active Node is already on PATH; it just prepends the same dirs.
$env:NVM_HOME = "C:\Users\fiffa\AppData\Local\nvm"
$env:NVM_SYMLINK = "C:\nvm4w\nodejs"
$env:Path = "$env:NVM_HOME;$env:NVM_SYMLINK;$env:Path"

# Worker config — key path is baked in for one-click boot.
$env:WORKER_ENABLED = "true"
$env:FIREBASE_SERVICE_ACCOUNT_FILE = "C:\Users\fiffa\firebase-key.json"

# Use the directory this script lives in as cwd so `npm run worker` finds
# the right package.json regardless of where it was invoked from.
Set-Location $PSScriptRoot

Write-Host ""
Write-Host "Starting AI editing worker..." -ForegroundColor Cyan
Write-Host "  Node:           $(node -v 2>$null)" -ForegroundColor DarkGray
Write-Host "  Service account: $env:FIREBASE_SERVICE_ACCOUNT_FILE" -ForegroundColor DarkGray
Write-Host "  Press Ctrl+C to stop." -ForegroundColor DarkGray
Write-Host ""

npm run worker
