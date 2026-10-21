$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$resolverPath = Join-Path $PSScriptRoot '..\scripts\Resolve-JsonSourceRoot.ps1'
$testRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('json-source-root-test-' + [guid]::NewGuid().ToString('N'))
$relativeSource = 'US Product\US_Application_Data'
$environmentNames = @('OneDriveCommercial', 'OneDrive', 'OneDriveConsumer', 'USERPROFILE')
$savedEnvironment = @{}
foreach ($environmentName in $environmentNames) {
    $savedEnvironment[$environmentName] = [Environment]::GetEnvironmentVariable($environmentName, 'Process')
}
$script:fixtureDrives = @()
$script:fixtureAccounts = @()

function Assert-True($Condition, $Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Path($Actual, $Expected, $Message) {
    $expectedFullPath = [System.IO.Path]::GetFullPath($Expected).TrimEnd('\')
    Assert-True ($Actual -is [string] -and $Actual.TrimEnd('\') -ieq $expectedFullPath) "$Message. Expected '$expectedFullPath', got '$Actual'."
}

function Assert-Throws($Action, $ExpectedMessage, $Message) {
    $caught = $null
    try { & $Action | Out-Null } catch { $caught = $_ }
    Assert-True ($null -ne $caught -and $caught.ToString().Contains($ExpectedMessage)) "$Message. Expected '$ExpectedMessage', got '$caught'."
}

function Reset-Discovery {
    foreach ($environmentName in $environmentNames) {
        [Environment]::SetEnvironmentVariable($environmentName, $null, 'Process')
    }
    $script:fixtureDrives = @()
    $script:fixtureAccounts = @()
}

function New-SourceFolder($RelativeRoot) {
    $sourcePath = Join-Path (Join-Path $testRoot $RelativeRoot) $relativeSource
    New-Item -ItemType Directory -Path $sourcePath -Force | Out-Null
    return $sourcePath
}

# Keep discovery deterministic: no real drive or account is searched by these tests.
function Get-PSDrive {
    [CmdletBinding()]
    param([string[]]$PSProvider)
    return $fixtureDrives
}

function Get-ChildItem {
    [CmdletBinding()]
    param(
        [string[]]$Path,
        [string[]]$LiteralPath,
        [switch]$Directory,
        [string]$Filter
    )
    $requestedPaths = if ($PSBoundParameters.ContainsKey('LiteralPath')) { $LiteralPath } else { $Path }
    if (@($requestedPaths | Where-Object { $_ -like 'HKCU:*' }).Count -gt 0) {
        return $fixtureAccounts
    }
    return Microsoft.PowerShell.Management\Get-ChildItem @PSBoundParameters
}

function Get-ItemProperty {
    [CmdletBinding()]
    param([string]$Path, [string]$LiteralPath, [string[]]$Name)
    $requestedPath = if ($PSBoundParameters.ContainsKey('LiteralPath')) { $LiteralPath } else { $Path }
    $account = @($fixtureAccounts | Where-Object { $_.PSPath -eq $requestedPath })
    if ($account.Count -gt 0) {
        return [pscustomobject]@{ UserFolder = $account[0].UserFolder }
    }
    return Microsoft.PowerShell.Management\Get-ItemProperty @PSBoundParameters
}

try {
    New-Item -ItemType Directory -Path $testRoot | Out-Null
    . $resolverPath
    Reset-Discovery

    Assert-Throws { Resolve-JsonSourceRoot } 'Could not find' 'Missing source did not give an actionable error'
    $firstSource = New-SourceFolder 'profile with spaces\OneDrive - Savvy Loan Products Ltd'
    $firstRoot = Split-Path (Split-Path $firstSource -Parent) -Parent
    $env:OneDriveCommercial = $firstRoot
    Assert-Path (Resolve-JsonSourceRoot) $firstSource 'Business OneDrive environment root was not used'

    # Aliases and a different case/trailing slash must still identify one source.
    $env:OneDrive = $firstRoot.ToUpperInvariant() + '\'
    $env:OneDriveConsumer = $firstRoot
    Assert-Path (Resolve-JsonSourceRoot) $firstSource 'Aliases were treated as multiple source folders'

    $secondSource = New-SourceFolder 'another profile\OneDrive - Savvy Loan Products Ltd'
    $secondRoot = Split-Path (Split-Path $secondSource -Parent) -Parent
    $env:OneDriveConsumer = $secondRoot
    Assert-Throws { Resolve-JsonSourceRoot } 'Multiple source folders' 'Ambiguous sources were silently selected'
    Assert-Path (Resolve-JsonSourceRoot -SourceRoot $secondSource) $secondSource 'Explicit path did not override ambiguous discovery'
    Assert-Throws { Resolve-JsonSourceRoot -SourceRoot (Join-Path $testRoot 'missing source') } 'SourceRoot does not exist or is not a folder' 'Invalid explicit path silently fell back to discovery'
    $filePath = Join-Path $testRoot 'source-is-a-file'
    [System.IO.File]::WriteAllText($filePath, 'not a source directory')
    Assert-Throws { Resolve-JsonSourceRoot -SourceRoot $filePath } 'SourceRoot does not exist or is not a folder' 'A file was accepted as the source folder'

    Reset-Discovery
    $env:OneDriveCommercial = Join-Path $testRoot 'stale OneDrive root'
    $env:OneDrive = $firstRoot
    Assert-Path (Resolve-JsonSourceRoot) $firstSource 'Stale configured roots prevented discovery'

    Reset-Discovery
    $env:OneDriveConsumer = $secondRoot
    Assert-Path (Resolve-JsonSourceRoot) $secondSource 'Consumer OneDrive environment root was not used'

    Reset-Discovery
    $profileSource = New-SourceFolder 'different user name\OneDrive - Savvy Loan Products Ltd'
    $env:USERPROFILE = Join-Path $testRoot 'different user name'
    Assert-Path (Resolve-JsonSourceRoot) $profileSource 'Current profile fallback used a hardcoded user name'

    Reset-Discovery
    $registrySource = New-SourceFolder 'registry account on another drive'
    $registryRoot = Split-Path (Split-Path $registrySource -Parent) -Parent
    $script:fixtureAccounts = @([pscustomobject]@{
        PSPath = 'HKCU:\Software\Microsoft\OneDrive\Accounts\Business1'
        UserFolder = $registryRoot
    })
    Assert-Path (Resolve-JsonSourceRoot) $registrySource 'OneDrive registry account was not discovered'

    # Each mounted-drive fallback is isolated, without scanning unrelated directories.
    foreach ($fallback in @('OneDrive - Savvy Loan Products Ltd\US Product\US_Application_Data', 'US Product\US_Application_Data', 'US_Application_Data')) {
        Reset-Discovery
        $mountedRoot = Join-Path $testRoot ('mounted-drive-' + [guid]::NewGuid().ToString('N'))
        $mountedSource = Join-Path $mountedRoot $fallback
        New-Item -ItemType Directory -Path $mountedSource -Force | Out-Null
        $script:fixtureDrives = @([pscustomobject]@{ Name = 'Fixture'; Root = $mountedRoot + '\'; Provider = 'FileSystem' })
        Assert-Path (Resolve-JsonSourceRoot) $mountedSource "Mounted drive fallback failed for '$fallback'"
    }

    Reset-Discovery
    $mountedRoot = Join-Path $testRoot 'rebased-drive'
    $configuredTail = 'json-source-root-moved-' + [guid]::NewGuid().ToString('N') + '\different profile\OneDrive - Savvy Loan Products Ltd'
    $env:OneDriveCommercial = Join-Path ([System.IO.Path]::GetPathRoot($testRoot)) $configuredTail
    $rebasedSource = Join-Path (Join-Path $mountedRoot $configuredTail) $relativeSource
    New-Item -ItemType Directory -Path $rebasedSource -Force | Out-Null
    $script:fixtureDrives = @([pscustomobject]@{ Name = 'Fixture'; Root = $mountedRoot + '\'; Provider = 'FileSystem' })
    Assert-Path (Resolve-JsonSourceRoot) $rebasedSource 'Configured OneDrive path was not rebased onto the available drive'

    Reset-Discovery
    $integrationSource = New-SourceFolder 'integration profile\OneDrive - Savvy Loan Products Ltd'
    $env:OneDriveCommercial = Split-Path (Split-Path $integrationSource -Parent) -Parent
    $applicationFolder = Join-Path $integrationSource '2026-10-20\Application'
    New-Item -ItemType Directory -Path $applicationFolder | Out-Null
    [System.IO.File]::WriteAllText((Join-Path $applicationFolder 'fixture.json'), '{}')
    $updaterPath = Join-Path $PSScriptRoot '..\scripts\Update-JsonZips.ps1'
    $result = & $updaterPath -Recurse -WhatIf
    Assert-True ($result.DateFoldersScanned -eq 1 -and $result.MatchingSubFoldersScanned -eq 1) 'Updater did not scan the automatically discovered fixture'
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $integrationSource '_json_zips'))) 'Automatic discovery with WhatIf created output'

    # A stale setting may refer to a drive that does not exist on this machine.
    Reset-Discovery
    $unusedDriveLetter = @('Z', 'Y', 'X', 'W', 'V', 'U', 'T', 'S', 'R', 'Q') |
        Where-Object { -not (Microsoft.PowerShell.Management\Get-PSDrive -Name $_ -ErrorAction SilentlyContinue) } |
        Select-Object -First 1
    Assert-True (-not [string]::IsNullOrWhiteSpace($unusedDriveLetter)) 'No unused drive letter available for stale-drive regression'
    $env:OneDriveCommercial = $unusedDriveLetter + ':\' + $configuredTail
    $script:fixtureDrives = @([pscustomobject]@{ Name = 'Fixture'; Root = $mountedRoot + '\'; Provider = 'FileSystem' })
    Assert-Path (Resolve-JsonSourceRoot) $rebasedSource 'An absent drive letter prevented OneDrive path rebasing'
    Write-Host 'All JSON source-root discovery tests passed.'
} finally {
    foreach ($environmentName in $environmentNames) {
        [Environment]::SetEnvironmentVariable($environmentName, $savedEnvironment[$environmentName], 'Process')
    }
    $resolvedTestRoot = [System.IO.Path]::GetFullPath($testRoot)
    $tempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath()).TrimEnd('\') + '\'
    if ($resolvedTestRoot.StartsWith($tempRoot, [System.StringComparison]::OrdinalIgnoreCase) -and
        (Split-Path $resolvedTestRoot -Leaf) -like 'json-source-root-test-*' -and
        (Test-Path -LiteralPath $resolvedTestRoot -PathType Container)) {
        Remove-Item -LiteralPath $resolvedTestRoot -Recurse -Force
    }
}
