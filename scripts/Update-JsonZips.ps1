[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [ValidateNotNullOrEmpty()]
    [string]$SourceRoot,

    [ValidateNotNullOrEmpty()]
    [string]$OutputRoot,

    [ValidateNotNullOrEmpty()]
    [string]$DateFolderPattern = '*',

    [ValidateNotNullOrEmpty()]
    [string[]]$SubFolderNames = @(
        'Application',
        'EssentialsJsonAccept',
        'EssentialsJsonReject',
        'OpenBanking',
        'OneScoreAccept',
        'OneScoreReject'
    ),

    [ValidateNotNullOrEmpty()]
    [string[]]$RootFolderNames = @(
        'onescore_missing',
        'openbanking_missing'
    ),

    [switch]$Recurse,

    [ValidateRange(1, 10)]
    [int]$ReadAttempts = 3,

    [ValidateRange(0, 60)]
    [int]$RetryDelaySeconds = 5,

    [switch]$Force,

    # Partial date selections retain entries from unselected dates automatically.
    # Use this switch for an intentionally additive full-source update.
    [switch]$PreserveExistingEntries
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-UnresolvedFullPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    return [System.IO.Path]::GetFullPath(
        $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Path)
    )
}

function Get-RelativePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BasePath,

        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    $baseUri = [System.Uri]::new(($BasePath.TrimEnd('\') + '\'))
    $targetUri = [System.Uri]::new($TargetPath)
    return [System.Uri]::UnescapeDataString(
        $baseUri.MakeRelativeUri($targetUri).ToString()
    ).Replace('/', '\')
}

function ConvertTo-ZipEntryName {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RelativePath
    )

    return $RelativePath.Replace('\', '/')
}

function Copy-FileWithRetry {
    param([string]$Source, [string]$Destination)

    for ($attempt = 1; $attempt -le $ReadAttempts; $attempt++) {
        try {
            [System.IO.File]::Copy($Source, $Destination, $true)
            return
        } catch {
            $cause = $_.Exception.GetBaseException()
            if ($cause -isnot [System.IO.IOException] -or $attempt -eq $ReadAttempts) {
                throw "Could not copy '$Source' to '$Destination' after $attempt attempt(s). $($cause.Message) If this is a OneDrive file, ensure OneDrive is running and select 'Always keep on this device' for the source folder, then wait for downloading to finish and rerun."
            }
            Write-Warning "Copy failed for '$Source' (attempt $attempt/$ReadAttempts): $($cause.Message) Retrying in $RetryDelaySeconds seconds."
            Start-Sleep -Seconds $RetryDelaySeconds
        }
    }
}

function Get-StreamHash {
    param([System.IO.Stream]$Stream)
    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try { return [System.BitConverter]::ToString($algorithm.ComputeHash($Stream)) }
    finally { $algorithm.Dispose() }
}

function Get-FileContentHash {
    param([string]$Path)
    $stream = [System.IO.File]::OpenRead($Path)
    try { return Get-StreamHash -Stream $stream }
    finally { $stream.Dispose() }
}

function Get-NonReportReason {
    param([string]$Path)
    $text = [System.IO.File]::ReadAllText($Path).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return 'Empty report export' }
    if ($text -ceq 'Not Acceptable') { return 'Provider returned Not Acceptable' }
    return $null
}

. (Join-Path $PSScriptRoot 'Resolve-JsonSourceRoot.ps1')
$sourceFullPath = Resolve-JsonSourceRoot -SourceRoot $SourceRoot
Write-Host "Updating JSON zip files from: $sourceFullPath"

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path -Path $sourceFullPath -ChildPath '_json_zips'
}

$outputFullPath = Get-UnresolvedFullPath -Path $OutputRoot

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$dateFolders = @(Get-ChildItem -LiteralPath $sourceFullPath -Directory |
    Where-Object {
        $_.Name -like $DateFolderPattern -and
        $_.FullName -ine $outputFullPath -and
        $RootFolderNames -inotcontains $_.Name
    } |
    Sort-Object Name)

$fileSearchOption = if ($Recurse) {
    [System.IO.SearchOption]::AllDirectories
} else {
    [System.IO.SearchOption]::TopDirectoryOnly
}

$workByZip = @{}
$foldersScannedByZip = @{}
$matchingSubFoldersScanned = 0

foreach ($subFolderName in @($SubFolderNames) + @($RootFolderNames)) {
    $workByZip[$subFolderName] = @{}
    $foldersScannedByZip[$subFolderName] = 0
}

foreach ($dateFolder in $dateFolders) {
    # Some imports contain an extra same-named date directory, for example
    # 2026-09-10\2026-09-10\Application. Support both layouts.
    $dateSearchRoots = @($dateFolder.FullName)
    $nestedDatePath = Join-Path -Path $dateFolder.FullName -ChildPath $dateFolder.Name
    if (Test-Path -LiteralPath $nestedDatePath -PathType Container) {
        $dateSearchRoots += $nestedDatePath
    }

    foreach ($subFolderName in $SubFolderNames) {
        foreach ($dateSearchRoot in $dateSearchRoots) {
            $subFolderPath = Join-Path -Path $dateSearchRoot -ChildPath $subFolderName
            if (-not (Test-Path -LiteralPath $subFolderPath -PathType Container)) {
                continue
            }

            $matchingSubFoldersScanned++
            $foldersScannedByZip[$subFolderName]++
            $jsonFiles = @([System.IO.Directory]::EnumerateFiles(
                $subFolderPath,
                '*',
                $fileSearchOption
            ) | Where-Object { [System.IO.Path]::GetExtension($_) -in @('.json', '.xml') } | Sort-Object)

            foreach ($jsonFilePath in $jsonFiles) {
                $jsonFile = Get-Item -LiteralPath $jsonFilePath
                $relativeJsonPath = Get-RelativePath -BasePath $subFolderPath -TargetPath $jsonFile.FullName
                $entryName = ConvertTo-ZipEntryName -RelativePath $relativeJsonPath

                # If the same report path exists in multiple dated folders, the latest dated folder wins.
                $previousCandidates = @()
                if ($workByZip[$subFolderName].ContainsKey($entryName)) {
                    $previousCandidates = @($workByZip[$subFolderName][$entryName].Candidates)
                }
                $workByZip[$subFolderName][$entryName] = [pscustomobject]@{
                    File = $jsonFile
                    DateFolderName = $dateFolder.Name
                    Candidates = @($previousCandidates) + @($jsonFile)
                }
            }
        }
    }
}

foreach ($rootFolderName in $RootFolderNames) {
    $rootFolderPath = Join-Path -Path $sourceFullPath -ChildPath $rootFolderName
    if (-not (Test-Path -LiteralPath $rootFolderPath -PathType Container)) {
        continue
    }

    $matchingSubFoldersScanned++
    $foldersScannedByZip[$rootFolderName]++
    $sourceFiles = @([System.IO.Directory]::EnumerateFiles(
        $rootFolderPath,
        '*',
        $fileSearchOption
    ) | Where-Object { [System.IO.Path]::GetExtension($_) -in @('.json', '.xml') } | Sort-Object)

    foreach ($sourceFilePath in $sourceFiles) {
        $sourceFile = Get-Item -LiteralPath $sourceFilePath
        $relativePath = Get-RelativePath -BasePath $rootFolderPath -TargetPath $sourceFile.FullName
        $entryName = ConvertTo-ZipEntryName -RelativePath $relativePath
        $workByZip[$rootFolderName][$entryName] = [pscustomobject]@{
            File = $sourceFile
            DateFolderName = $null
            Candidates = @($sourceFile)
        }
    }
}

$mirrorInventory = -not $PreserveExistingEntries -and $DateFolderPattern -eq '*'
if ($mirrorInventory) {
    # Validate every existing archive before publishing any archive. An absent
    # source folder can mean a wrong root or an incomplete OneDrive inventory.
    # A folder that exists and is intentionally empty remains a valid snapshot.
    foreach ($subFolderName in @($SubFolderNames) + @($RootFolderNames)) {
        $zipBaseName = if ($subFolderName -ieq 'Application') { 'Applications' } else { $subFolderName }
        $zipPath = Join-Path -Path $outputFullPath -ChildPath ($zipBaseName + '.zip')
        if ($foldersScannedByZip[$subFolderName] -eq 0 -and (Test-Path -LiteralPath $zipPath -PathType Leaf)) {
            throw "Full source snapshot aborted before publication: no '$subFolderName' source folder was found, but '$zipPath' already exists. No published archives were changed. Check SourceRoot and the downloaded source folders, or use -PreserveExistingEntries for an intentional additive update."
        }
    }
}

$summary = [ordered]@{
    DateFoldersScanned = $dateFolders.Count
    MatchingSubFoldersScanned = $matchingSubFoldersScanned
    ZipsTouched = 0
    FilesAdded = 0
    FilesUpdated = 0
    FilesSkipped = 0
    LegacyDateEntriesRemoved = 0
    StaleEntriesRemoved = 0
    NonReportsExcluded = 0
    SnapshotMode = if ($PreserveExistingEntries -or $DateFolderPattern -ne '*') { 'Partial update' } else { 'Full source snapshot' }
}
$zipsTouched = @{}
$dateEntryPrefixes = @($dateFolders | ForEach-Object { $_.Name + '/' })

foreach ($subFolderName in @($SubFolderNames) + @($RootFolderNames)) {
    $entriesToWrite = $workByZip[$subFolderName]
    $zipBaseName = if ($subFolderName -ieq 'Application') { 'Applications' } else { $subFolderName }
    $zipPath = Join-Path -Path $outputFullPath -ChildPath ($zipBaseName + '.zip')
    $zipExists = Test-Path -LiteralPath $zipPath -PathType Leaf
    # Do not manufacture empty archives for source roles absent from this root.
    if ($foldersScannedByZip[$subFolderName] -eq 0 -and -not $zipExists) { continue }

    if ($PSCmdlet.ShouldProcess($zipPath, "Update flat JSON zip for $subFolderName")) {
        if (-not (Test-Path -LiteralPath $outputFullPath -PathType Container)) {
            New-Item -ItemType Directory -Path $outputFullPath | Out-Null
        }

        # Work locally so a failed cloud read never changes the published archive.
        $workingZipPath = [System.IO.Path]::GetTempFileName()
        $stagedFilePath = [System.IO.Path]::GetTempFileName()
        $publishPath = Join-Path $outputFullPath ('.' + $zipBaseName + '.' + [guid]::NewGuid().ToString('N') + '.tmp')
        try {
            Write-Host "Updating $zipBaseName.zip ($($entriesToWrite.Count) source files)..."
            if ($zipExists) {
                Copy-FileWithRetry -Source $zipPath -Destination $workingZipPath
            }
            # Hash through read-only streams before opening Update mode. Update
            # streams buffer expanded entry bodies, which is costly for large ZIPs.
            $previousEntryHashes = @{}
            if ($zipExists -and -not $Force) {
                $comparisonZip = [System.IO.Compression.ZipFile]::OpenRead($workingZipPath)
                try {
                    foreach ($comparisonEntry in $comparisonZip.Entries) {
                        if (-not $entriesToWrite.ContainsKey($comparisonEntry.FullName)) { continue }
                        $comparisonStream = $comparisonEntry.Open()
                        try { $comparisonHash = Get-StreamHash -Stream $comparisonStream }
                        finally { $comparisonStream.Dispose() }
                        $previousEntryHashes[$comparisonEntry.FullName] = $comparisonHash
                    }
                } finally { $comparisonZip.Dispose() }
            }
            $zipStream = [System.IO.File]::Open(
                $workingZipPath,
                [System.IO.FileMode]::OpenOrCreate,
                [System.IO.FileAccess]::ReadWrite,
                [System.IO.FileShare]::None
            )

            try {
                $zip = [System.IO.Compression.ZipArchive]::new(
                    $zipStream,
                    [System.IO.Compression.ZipArchiveMode]::Update
                )
                try {
                    $zipTouched = -not $zipExists

                    $legacyEntries = @($zip.Entries | Where-Object {
                        $entryFullName = $_.FullName
                        $dateEntryPrefixes | Where-Object { $entryFullName.StartsWith($_, [System.StringComparison]::OrdinalIgnoreCase) }
                    })

                    foreach ($legacyEntry in $legacyEntries) {
                        $legacyEntry.Delete()
                        $summary.LegacyDateEntriesRemoved++
                        $zipTouched = $true
                    }

                    if ($mirrorInventory) {
                        # The complete source inventory owns this archive. Do not
                        # retain old flat XML/JSON entries that no longer exist.
                        $staleEntries = @($zip.Entries | Where-Object { -not $entriesToWrite.ContainsKey($_.FullName) })
                        foreach ($staleEntry in $staleEntries) {
                            $staleEntry.Delete()
                            $summary.StaleEntriesRemoved++
                            $zipTouched = $true
                        }
                    }

                    $existingEntriesByName = @{}
                    foreach ($existingEntry in $zip.Entries) {
                        if (-not $existingEntriesByName.ContainsKey($existingEntry.FullName)) {
                            $existingEntriesByName[$existingEntry.FullName] = @()
                        }
                        $existingEntriesByName[$existingEntry.FullName] += @($existingEntry)
                    }

                    foreach ($entryName in ($entriesToWrite.Keys | Sort-Object)) {
                        $existingEntries = @()
                        if ($existingEntriesByName.ContainsKey($entryName)) {
                            $existingEntries = @($existingEntriesByName[$entryName])
                        }
                        $sourceCandidates = @($entriesToWrite[$entryName].Candidates)
                        [array]::Reverse($sourceCandidates)
                        $jsonFile = $null
                        foreach ($sourceCandidate in $sourceCandidates) {
                            # Read every chosen source fully before changing the staged ZIP.
                            Copy-FileWithRetry -Source $sourceCandidate.FullName -Destination $stagedFilePath
                            $nonReportReason = Get-NonReportReason -Path $stagedFilePath
                            if ($null -eq $nonReportReason) {
                                $jsonFile = $sourceCandidate
                                break
                            }
                            $summary.NonReportsExcluded++
                            Write-Warning "$nonReportReason; excluded source '$($sourceCandidate.FullName)' from $zipBaseName.zip`: $entryName"
                        }
                        if ($null -eq $jsonFile) {
                            foreach ($existingEntry in $existingEntries) {
                                $existingEntry.Delete()
                                $summary.StaleEntriesRemoved++
                                $zipTouched = $true
                            }
                            continue
                        }

                        $shouldWrite = $Force -or $existingEntries.Count -ne 1
                        if (-not $shouldWrite) {
                            $shouldWrite = $previousEntryHashes[$entryName] -ne (Get-FileContentHash -Path $stagedFilePath)
                        }
                        if (-not $shouldWrite) {
                            $summary.FilesSkipped++
                            continue
                        }

                        if ($existingEntries.Count -gt 0) {
                            foreach ($existingEntry in $existingEntries) { $existingEntry.Delete() }
                            $summary.FilesUpdated++
                        } else {
                            $summary.FilesAdded++
                        }

                        $newEntry = [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                            $zip,
                            $stagedFilePath,
                            $entryName,
                            [System.IO.Compression.CompressionLevel]::Optimal
                        )
                        $newEntry.LastWriteTime = $jsonFile.LastWriteTimeUtc

                        $zipTouched = $true
                    }

                    if ($zipTouched) {
                        $zipsTouched[$zipPath] = $true
                    }
                } finally {
                    $zip.Dispose()
                }
            } finally {
                $zipStream.Dispose()
            }
            if ($zipTouched) {
                Copy-FileWithRetry -Source $workingZipPath -Destination $publishPath
                # Replace on the destination volume only after the complete zip is closed.
                if ($zipExists) {
                    [System.IO.File]::Replace($publishPath, $zipPath, [System.Management.Automation.Language.NullString]::Value)
                } else {
                    [System.IO.File]::Move($publishPath, $zipPath)
                }
            }
        } finally {
            foreach ($temporaryPath in @($workingZipPath, $stagedFilePath, $publishPath)) {
                if ([System.IO.File]::Exists($temporaryPath)) {
                    [System.IO.File]::Delete($temporaryPath)
                }
            }
        }
    }
}

$summary.ZipsTouched = $zipsTouched.Count
[pscustomobject]$summary

