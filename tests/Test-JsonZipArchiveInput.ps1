$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$scriptPath = Join-Path $PSScriptRoot '..\scripts\Update-JsonZips.ps1'
$testRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('json-zip-archive-test-' + [guid]::NewGuid().ToString('N'))

function Assert-True($Condition, $Message) {
    if (-not $Condition) { throw $Message }
}

function Write-TestZip($Path, $Entries) {
    $parent = Split-Path $Path -Parent
    if (-not (Test-Path -LiteralPath $parent -PathType Container)) {
        New-Item -ItemType Directory -Path $parent | Out-Null
    }
    if (Test-Path -LiteralPath $Path) { Remove-Item -LiteralPath $Path -Force }
    $archive = [System.IO.Compression.ZipFile]::Open($Path, 'Create')
    try {
        foreach ($item in $Entries) {
            $entry = $archive.CreateEntry($item.Name)
            if ($null -eq $item.Content) { continue }
            $writer = [System.IO.StreamWriter]::new($entry.Open())
            try { $writer.Write($item.Content) } finally { $writer.Dispose() }
        }
    } finally { $archive.Dispose() }
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

function Get-ZipEntryNames($Path) {
    $archive = [System.IO.Compression.ZipFile]::OpenRead($Path)
    try { return @($archive.Entries | ForEach-Object FullName) }
    finally { $archive.Dispose() }
}

try {
    New-Item -ItemType Directory -Path $testRoot | Out-Null
    Write-TestZip (Join-Path $testRoot '2026-11-01.zip') @(
        [pscustomobject]@{ Name = '2026-11-01/Application/'; Content = $null },
        [pscustomobject]@{ Name = '2026-11-01/Application/same.json'; Content = '{"source":"old"}' },
        [pscustomobject]@{ Name = '2026-11-01/OpenBanking/'; Content = $null },
        [pscustomobject]@{ Name = '2026-11-01/OpenBanking/report.xml'; Content = '<valid/>' }
    )
    Write-TestZip (Join-Path $testRoot '2026-11-02.zip') @(
        [pscustomobject]@{ Name = '2026-11-02/2026-11-02/Application/'; Content = $null },
        [pscustomobject]@{ Name = '2026-11-02/2026-11-02/Application/same.json'; Content = 'archive-new' },
        [pscustomobject]@{ Name = '2026-11-02/2026-11-02/OpenBanking/'; Content = $null },
        [pscustomobject]@{ Name = '2026-11-02/2026-11-02/OpenBanking/report.xml'; Content = 'Not Acceptable' },
        [pscustomobject]@{ Name = '2026-11-02/unclassified.json'; Content = '{}' }
    )

    # The archive must win over a stale extracted copy for the same date.
    $extracted = Join-Path $testRoot '2026-11-02\Application'
    New-Item -ItemType Directory -Path $extracted | Out-Null
    [System.IO.File]::WriteAllText((Join-Path $extracted 'same.json'), 'extracted-old')

    $oneFolder = Join-Path $testRoot 'onescore_missing'
    New-Item -ItemType Directory -Path $oneFolder | Out-Null
    [System.IO.File]::WriteAllText((Join-Path $oneFolder 'one.xml'), '<extracted/>')
    Write-TestZip (Join-Path $testRoot 'onescore_missing.zip') @(
        [pscustomobject]@{ Name = 'onescore_missing/'; Content = $null },
        [pscustomobject]@{ Name = 'onescore_missing/one.xml'; Content = '<archive/>' },
        [pscustomobject]@{ Name = 'onescore_missing/notes.mht'; Content = 'ignored' },
        [pscustomobject]@{ Name = 'onescore_missing/nested.zip'; Content = 'ignored' }
    )
    Write-TestZip (Join-Path $testRoot 'openbanking_missing.zip') @(
        [pscustomobject]@{ Name = 'openbanking_missing/'; Content = $null },
        [pscustomobject]@{ Name = 'openbanking_missing/missing.json'; Content = '{"ok":true}' }
    )

    $arguments = @{
        SourceRoot = $testRoot
        Recurse = $true
        SubFolderNames = @('Application', 'OpenBanking')
        RootFolderNames = @('onescore_missing', 'openbanking_missing')
    }
    $result = & $scriptPath @arguments -WarningAction SilentlyContinue
    $output = Join-Path $testRoot '_json_zips'
    Assert-True ($result.DateArchivesScanned -eq 2) 'Daily source archives were not counted'
    Assert-True ($result.NonReportsExcluded -eq 1) 'Invalid newer archive report was not excluded'
    Assert-True ((Read-ZipText (Join-Path $output 'Applications.zip') 'same.json') -eq 'archive-new') 'Latest archive did not beat extracted source'
    Assert-True ((Read-ZipText (Join-Path $output 'OpenBanking.zip') 'report.xml') -eq '<valid/>') 'Older valid archive report was not used as fallback'
    Assert-True ((Read-ZipText (Join-Path $output 'onescore_missing.zip') 'one.xml') -eq '<archive/>') 'Root archive did not beat extracted source'
    Assert-True ((Read-ZipText (Join-Path $output 'openbanking_missing.zip') 'missing.json') -eq '{"ok":true}') 'Root JSON archive entry missing'
    $oneNames = Get-ZipEntryNames (Join-Path $output 'onescore_missing.zip')
    Assert-True ('notes.mht' -notin $oneNames -and 'nested.zip' -notin $oneNames) 'Non-report archive entries were included'

    $result = & $scriptPath @arguments -WarningAction SilentlyContinue
    Assert-True ($result.FilesSkipped -eq 4 -and $result.ZipsTouched -eq 0) 'Unchanged archive sources were not skipped'

    # A locked/cloud-unavailable source archive must fail before publication,
    # and all source archive handles from earlier runs must have been released.
    $applicationZip = Join-Path $output 'Applications.zip'
    $beforeLockedRun = (Get-FileHash -LiteralPath $applicationZip).Hash
    $lockedSourceZip = Join-Path $testRoot '2026-11-02.zip'
    $lock = [System.IO.File]::Open($lockedSourceZip, 'Open', 'ReadWrite', 'None')
    $caught = $null
    $retryWarnings = @()
    try {
        & $scriptPath @arguments -ReadAttempts 2 -RetryDelaySeconds 0 -WarningVariable retryWarnings | Out-Null
    } catch { $caught = $_ } finally { $lock.Dispose() }
    Assert-True ($null -ne $caught -and $caught.ToString().Contains($lockedSourceZip)) 'Locked source archive failure was not reported'
    Assert-True ($retryWarnings.Count -eq 1) 'Locked source archive was not retried'
    Assert-True ((Get-FileHash -LiteralPath $applicationZip).Hash -eq $beforeLockedRun) 'Locked source archive changed published output'

    # Replacing the input archive with same-length changed content must update.
    Write-TestZip (Join-Path $testRoot '2026-11-02.zip') @(
        [pscustomobject]@{ Name = '2026-11-02/Application/'; Content = $null },
        [pscustomobject]@{ Name = '2026-11-02/Application/same.json'; Content = 'archive-two' },
        [pscustomobject]@{ Name = '2026-11-02/OpenBanking/'; Content = $null },
        [pscustomobject]@{ Name = '2026-11-02/OpenBanking/report.xml'; Content = 'Not Acceptable' }
    )
    $result = & $scriptPath @arguments -WarningAction SilentlyContinue
    Assert-True ((Read-ZipText (Join-Path $output 'Applications.zip') 'same.json') -eq 'archive-two') 'Same-length archive change was skipped'

    # Unsafe entry paths must be rejected before any output is published.
    $unsafeRoot = Join-Path $testRoot 'unsafe'
    Write-TestZip (Join-Path $unsafeRoot '2026-11-03.zip') @(
        [pscustomobject]@{ Name = '2026-11-03/Application/../escape.json'; Content = '{}' }
    )
    $caught = $null
    try {
        & $scriptPath -SourceRoot $unsafeRoot -SubFolderNames @('Application') -RootFolderNames @('unused') | Out-Null
    } catch { $caught = $_ }
    Assert-True ($null -ne $caught -and $caught.ToString().Contains('Unsafe source archive entry path')) 'Unsafe archive path was accepted'
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $unsafeRoot '_json_zips'))) 'Unsafe archive created output'

    $duplicateRoot = Join-Path $testRoot 'duplicate'
    Write-TestZip (Join-Path $duplicateRoot '2026-11-04.zip') @(
        [pscustomobject]@{ Name = '2026-11-04/Application/same.json'; Content = '{"first":true}' },
        [pscustomobject]@{ Name = 'Application/same.json'; Content = '{"second":true}' }
    )
    $caught = $null
    try {
        & $scriptPath -SourceRoot $duplicateRoot -SubFolderNames @('Application') -RootFolderNames @('unused') | Out-Null
    } catch { $caught = $_ }
    Assert-True ($null -ne $caught -and $caught.ToString().Contains('Duplicate source archive mapping')) 'Duplicate logical archive entry was accepted'
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $duplicateRoot '_json_zips'))) 'Duplicate archive created output'

    Write-Host 'All direct ZIP source tests passed.'
} finally {
    $resolvedTestRoot = [System.IO.Path]::GetFullPath($testRoot)
    $tempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath()).TrimEnd('\') + '\'
    if ($resolvedTestRoot.StartsWith($tempRoot, [System.StringComparison]::OrdinalIgnoreCase) -and
        (Split-Path $resolvedTestRoot -Leaf) -like 'json-zip-archive-test-*') {
        Remove-Item -LiteralPath $resolvedTestRoot -Recurse -Force
    }
}
