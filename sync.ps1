# Copies class folders under C:\ into this repo, skipping virtualenvs and generated files.
# Usage (from the repo root):  powershell -ExecutionPolicy Bypass -File .\sync.ps1
# To add a new class, append one line to $Map: source folder => repo folder.

$ErrorActionPreference = 'Stop'
$RepoRoot = $PSScriptRoot

$Map = [ordered]@{
    'C:\pybo\main' = '01_pybo\main'
    'C:\jsonpd'    = '02_jsonpd'
    'C:\dstd'      = '03_dstd'
    'C:\sparkcamp' = '04_sparkcamp'
}

# Directory names skipped at any depth
$ExcludeDirs = @(
    '.venv', '.venv-1', 'venv', '__pycache__', '.ipynb_checkpoints', '.git',
    'tmp', 'spark-warehouse', 'out', 'metastore_db', 'ivy', 'lake'
)
# File patterns skipped
$ExcludeFiles = @('*.pyc', '*.crc', 'db.sqlite3', '.env', 'derby.log', '*.log', '*.exe', '*.dll')

foreach ($src in $Map.Keys) {
    $dst = Join-Path $RepoRoot $Map[$src]
    if (-not (Test-Path $src)) {
        Write-Warning "Skip (source not found): $src"
        continue
    }
    Write-Host "Sync $src -> $dst"
    # /MIR mirrors (also removes files deleted at the source); excluded items are left alone
    robocopy $src $dst /MIR /XD $ExcludeDirs /XF $ExcludeFiles /NFL /NDL /NJH /NP /R:1 /W:1 | Out-Host
    # robocopy exit codes 0-7 are success, 8+ are failures
    if ($LASTEXITCODE -ge 8) { throw "robocopy failed for $src (exit $LASTEXITCODE)" }
}
$global:LASTEXITCODE = 0
Write-Host "Done. Review with: git status"
