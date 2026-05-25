$ErrorActionPreference = "Stop"

$AppDir = "C:\Experiments\Claude\TradingApp"
$Port = "5001"
$Python = Join-Path $AppDir ".venv\Scripts\python.exe"
$Pip = Join-Path $AppDir ".venv\Scripts\pip.exe"
$LogOut = Join-Path $AppDir "server.out.log"
$LogErr = Join-Path $AppDir "server.err.log"

Set-Location $AppDir

if (-not (Test-Path -LiteralPath $Python)) {
    python -m venv .venv
}

& $Pip install -r requirements.txt

$env:PORT = $Port
& $Python -u app.py 1>> $LogOut 2>> $LogErr
