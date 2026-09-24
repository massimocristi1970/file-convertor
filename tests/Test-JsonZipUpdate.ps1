$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.IO.Compression.FileSystem
$scriptPath = Join-Path $PSScriptRoot '..\scripts\Update-JsonZips.ps1'
$testRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('json-zip-test-' + [guid]::NewGuid().ToString('N'))

function Assert-True($Condition, $Message) {
    if (-not $Condition) { throw $Message }
}

function Read-ZipText($Path, $Name) {
    $archive = [System.IO.Compression.ZipFile]::OpenRead($Path)
    try {
        $entry = $archive.GetEntry($Name)
        Assert-True ($null -ne $entry) "Missing entry: $Name"
        $reader = [System.IO.StreamReader]::new($entry.Open())
        try { return $reader.ReadToEnd() } finally { $reader.Dispose() }
    } finally { $archive.Dispose() }
}

try {
    $older = Join-Path $testRoot '2026-09-01\Application'
    $newer = Join-Path $testRoot '2026-09-02\2026-09-02\Application'
    $missing = Join-Path $testRoot 'onescore_missing'
    New-Item -ItemType Directory -Path $older, (Join-Path $newer 'nested'), $missing | Out-Null
    [System.IO.File]::WriteAllText((Join-Path $older 'same.json'), '{"version":1}')
    $sourceFile = Join-Path $newer 'same.json'
    [System.IO.File]::WriteAllText($sourceFile, '{"version":2}')
    [System.IO.File]::WriteAllText((Join-Path $newer 'nested\child.json'), '{}')
    [System.IO.File]::WriteAllText((Join-Path $missing 'data.xml'), '<data/>')

    & $scriptPath -SourceRoot $testRoot -Recurse -WhatIf | Out-Null
    Assert-True (-not (Test-Path (Join-Path $testRoot '_json_zips'))) 'WhatIf created output'
    $result = & $scriptPath -SourceRoot $testRoot -Recurse
    $zipPath = Join-Path $testRoot '_json_zips\Applications.zip'
    Assert-True ($result.ZipsTouched -eq 8 -and $result.FilesAdded -eq 3) 'Initial summary incorrect'
    Assert-True ((Read-ZipText $zipPath 'same.json') -eq '{"version":2}') 'Latest dated file did not win'
    Assert-True ((Read-ZipText $zipPath 'nested/child.json') -eq '{}') 'Recursive entry missing'
    Assert-True ((Read-ZipText (Join-Path $testRoot '_json_zips\onescore_missing.zip') 'data.xml') -eq '<data/>') 'Root XML missing'
    $result = & $scriptPath -SourceRoot $testRoot -Recurse
    Assert-True ($result.FilesSkipped -eq 3 -and $result.ZipsTouched -eq 0) 'Unchanged files not skipped'

    $archive = [System.IO.Compression.ZipFile]::Open($zipPath, 'Update')
    try { $archive.CreateEntry('2026-09-01/legacy.json') | Out-Null } finally { $archive.Dispose() }
    $before = (Get-FileHash -LiteralPath $zipPath).Hash
    [System.IO.File]::WriteAllText($sourceFile, '{"version":333}')
    $lock = [System.IO.File]::Open($sourceFile, 'Open', 'ReadWrite', 'None')
    $caught = $null
    $retryWarnings = @()
    try {
        & $scriptPath -SourceRoot $testRoot -Recurse -ReadAttempts 2 -RetryDelaySeconds 0 -WarningVariable retryWarnings | Out-Null
    } catch { $caught = $_ } finally { $lock.Dispose() }
    Assert-True ($null -ne $caught -and $caught.ToString().Contains($sourceFile)) 'Failure did not identify source path'
    Assert-True ($retryWarnings.Count -eq 1) 'Failed read was not retried'
    Assert-True ((Get-FileHash -LiteralPath $zipPath).Hash -eq $before) 'Failed read changed published zip'
    Assert-True (@(Get-ChildItem (Join-Path $testRoot '_json_zips') -Filter '*.tmp' -Force).Count -eq 0) 'Publication temp files leaked'

    $result = & $scriptPath -SourceRoot $testRoot -Recurse
    Assert-True ($result.FilesUpdated -eq 1 -and $result.LegacyDateEntriesRemoved -eq 1) 'Update/legacy cleanup incorrect'
    Assert-True ((Read-ZipText $zipPath 'same.json') -eq '{"version":333}') 'Updated content incorrect'
    $archive = [System.IO.Compression.ZipFile]::OpenRead($zipPath)
    try { Assert-True ($null -eq $archive.GetEntry('2026-09-01/legacy.json')) 'Legacy entry retained' } finally { $archive.Dispose() }
    $result = & $scriptPath -SourceRoot $testRoot -Recurse -Force
    Assert-True ($result.FilesUpdated -eq 3) 'Force did not rewrite all source entries'
    Write-Host 'All JSON zip update tests passed.'
} finally {
    $resolvedTestRoot = [System.IO.Path]::GetFullPath($testRoot)
    $tempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath()).TrimEnd('\') + '\'
    if ($resolvedTestRoot.StartsWith($tempRoot, [System.StringComparison]::OrdinalIgnoreCase) -and
        (Split-Path $resolvedTestRoot -Leaf) -like 'json-zip-test-*') {
        Remove-Item -LiteralPath $resolvedTestRoot -Recurse -Force
    }
}
