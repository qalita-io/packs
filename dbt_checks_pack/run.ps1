# PowerShell runner for Qalita pack (Windows, no WSL required)
Param()
$ErrorActionPreference = 'Stop'

Write-Host ("Running as user: {0}" -f $env:USERNAME)

# Extract pack name from properties.yaml
Write-Host "Extracting pack name..."
$PACK_NAME = ($null)
try {
  $PACK_NAME = (Select-String -Path "properties.yaml" -Pattern '^\s*name:\s*(.+)$' | ForEach-Object { $_.Matches[0].Groups[1].Value.Trim() } | Select-Object -First 1)
} catch {}
if (-not $PACK_NAME) { Write-Error "Failed to extract pack name."; exit 1 }
Write-Host ("Pack name: {0}" -f $PACK_NAME)

# Resolve Python requirement from pyproject.toml
Write-Host "Resolving Python version from pyproject.toml..."
if (-not (Test-Path "pyproject.toml")) { Write-Error "pyproject.toml not found."; exit 1 }
$REQUIRED_SPEC = ($null)
try {
  $REQUIRED_SPEC = (Select-String -Path "pyproject.toml" -Pattern '^\s*python\s*=\s*\"?(.+?)\"?\s*$' | Select-Object -First 1).Matches.Groups[1].Value.Trim()
} catch {}
if (-not $REQUIRED_SPEC) { Write-Error "Could not read python requirement from pyproject.toml."; exit 1 }
Write-Host ("Python requirement: {0}" -f $REQUIRED_SPEC)

$MIN_VER = ([regex]::Match($REQUIRED_SPEC, '>=\s*([0-9]+\.[0-9]+)')).Groups[1].Value
$MAX_VER = ([regex]::Match($REQUIRED_SPEC, '<\s*([0-9]+\.[0-9]+)')).Groups[1].Value

function VersionGE([string]$a,[string]$b) { return ([version]($a + '.0')) -ge ([version]($b + '.0')) }
function VersionLT([string]$a,[string]$b) { return ([version]($a + '.0')) -lt ([version]($b + '.0')) }

# Build candidate list via py launcher and fallbacks
$candidates = @()
try {
  $out = & py -0p 2>$null
  foreach ($l in $out) {
    if ($l -match '^\s*-\s*([0-9]+\.[0-9]+)\s*:\s*(.+)$') {
      $v = $Matches[1]; $p = $Matches[2]
      if ($v -like '3.*') { $candidates += @{ ver=$v; path=$p } }
    }
  }
} catch {}

foreach ($cmd in @('python3','python')) {
  $p = (Get-Command $cmd -ErrorAction SilentlyContinue).Path
  if ($p) {
    $vout = & $p -V 2>&1
    $v = ($vout -replace 'Python\s+','').Split()[0]
    $v = ($v -split '\.')[0..1] -join '.'
    if ($v -like '3.*') { $candidates += @{ ver=$v; path=$p } }
  }
}

$best = $null
foreach ($c in ($candidates | Sort-Object { [version]($_.ver + '.0') } -Descending)) {
  if ($MIN_VER -and -not (VersionGE $c.ver $MIN_VER)) { continue }
  if ($MAX_VER -and -not (VersionLT $c.ver $MAX_VER)) { continue }
  $best = $c; break
}
if (-not $best) { Write-Error ("No available Python interpreter satisfies requirement: {0}" -f $REQUIRED_SPEC); exit 1 }
$PYTHON_CMD = $best.path
$PYTHON_VERSION = $best.ver
Write-Host ("Selected Python: {0} (version {1})" -f $PYTHON_CMD, $PYTHON_VERSION)

# Install poetry if not available (download installer to temp to avoid STDIN issues)
$poetryCmd = (Get-Command poetry -ErrorAction SilentlyContinue)
$POETRY_BIN = $null
if (-not $poetryCmd) {
  Write-Host "Poetry could not be found, installing now..."
  if (-not $env:POETRY_HOME) { $env:POETRY_HOME = Join-Path $env:USERPROFILE ".poetry" }
  $installer = Join-Path $env:TEMP ("install-poetry-{0}.py" -f ([System.Guid]::NewGuid().ToString('N')))
  try {
    Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing -OutFile $installer
    & $PYTHON_CMD $installer
  } catch {
    Write-Host "Poetry installer failed, continuing without Poetry (will fallback to pip)."
  } finally {
    if (Test-Path $installer) { Remove-Item $installer -Force -ErrorAction SilentlyContinue }
  }
  # Try common locations
  $candidates = @()
  if ($env:POETRY_HOME) { $candidates += (Join-Path $env:POETRY_HOME "bin\poetry.exe") }
  $roaming = [Environment]::GetFolderPath('ApplicationData')
  if ($roaming) { $candidates += (Join-Path $roaming "Python\Scripts\poetry.exe") }
  $found = $null
  foreach ($c in $candidates) { if (Test-Path $c) { $found = $c; break } }
  if (-not $found) { $poetryCmd = (Get-Command poetry -ErrorAction SilentlyContinue) }
  $POETRY_BIN = if ($found) { $found } elseif ($poetryCmd) { $poetryCmd.Path } else { $null }
} else {
  $POETRY_BIN = $poetryCmd.Path
}

# Ensure export plugin (Poetry 2.x) if poetry is available
if ($POETRY_BIN) {
  try { & $POETRY_BIN self add poetry-plugin-export *> $null } catch {}
}

Write-Host ("Detected Python version: {0}" -f $PYTHON_VERSION)

# Build venv path
$QALITA_HOME = if ($env:QALITA_HOME) { $env:QALITA_HOME } else { Join-Path $env:USERPROFILE ".qalita" }
Write-Host ("Virtual Environment Root: {0}" -f $QALITA_HOME)
$VENV_PATH = Join-Path $QALITA_HOME ("agent_run_temp\{0}_py{1}_venv" -f $PACK_NAME, $PYTHON_VERSION)

if (-not (Test-Path $VENV_PATH)) {
  Write-Host "Creating virtual environment..."
  & $PYTHON_CMD -m venv "$VENV_PATH"
  if ($LASTEXITCODE -ne 0) { Write-Error "Failed to create virtual environment for $PACK_NAME."; exit 1 }
  Write-Host "Virtual environment created."
} else {
  Write-Host "Virtual environment already exists."
}

# Activate venv
Write-Host "Activating virtual environment..."
$activatePs1 = Join-Path $VENV_PATH "Scripts\Activate.ps1"
$activateBat = Join-Path $VENV_PATH "Scripts\activate.bat"
if (Test-Path $activatePs1) {
  . $activatePs1
} elseif (Test-Path $activateBat) {
  cmd /c `"$activateBat`"
}

if (-not (Get-Command python -ErrorAction SilentlyContinue)) { Write-Error "Failed to activate virtual environment."; exit 1 }
Write-Host ("Venv python: {0}" -f (Get-Command python).Path)
Write-Host ("Venv python version: {0}" -f (& python -V 2>&1))

# Install requirements using poetry export + pip (fallback to pip install .)
Write-Host "Installing requirements using poetry..."
$env:POETRY_VIRTUALENVS_CREATE = "false"
$env:PIP_DISABLE_PIP_VERSION_CHECK = "1"
python -m pip install --upgrade --quiet pip setuptools wheel

$REQ_FILE = Join-Path (Get-Location) ".qalita_requirements.txt"
$exportOk = $false
if ($POETRY_BIN) {
  try {
    & $POETRY_BIN export -f requirements.txt --without-hashes -o "$REQ_FILE" *> $null
    if ($LASTEXITCODE -eq 0 -and (Test-Path "$REQ_FILE") -and ((Get-Item "$REQ_FILE").Length -gt 0)) { $exportOk = $true }
  } catch {}
}
if ($exportOk) {
  python -m pip install -r "$REQ_FILE" --quiet
  if ($LASTEXITCODE -ne 0) { Write-Error "Failed to install requirements with pip."; exit 1 }
  Write-Host "Requirements installed from exported lock."
} else {
  Write-Host "Poetry unavailable or export failed; installing project with pip."
  python -m pip install .
  if ($LASTEXITCODE -ne 0) { Write-Error "Failed to install project with pip."; exit 1 }
  Write-Host "Project installed into venv."
}

# Run your script
Write-Host "Running script..."
python main.py
if ($LASTEXITCODE -ne 0) { Write-Error "Script execution failed."; exit 1 }
Write-Host "Script executed successfully."

# Deactivate virtual environment
Write-Host "Deactivating virtual environment..."
if (Get-Command deactivate -ErrorAction SilentlyContinue) { deactivate }
Write-Host "Virtual environment deactivated."


