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

function Get-SourceArchiveWithRetry {
    param([string]$Path)

    if ($script:sourceArchiveCache.ContainsKey($Path)) {
        return $script:sourceArchiveCache[$Path]
    }

    for ($attempt = 1; $attempt -le $ReadAttempts; $attempt++) {
        $archive = $null
        try {
            $archive = [System.IO.Compression.ZipFile]::OpenRead($Path)
            $entries = [System.Collections.Generic.List[object]]::new()
            $entryLookup = [System.Collections.Generic.Dictionary[string, object]]::new(
                [System.StringComparer]::Ordinal
            )
            $duplicateEntryNames = [System.Collections.Generic.HashSet[string]]::new(
                [System.StringComparer]::Ordinal
            )
            # Materialize and index the central directory inside the retry
            # boundary. Candidate reads can then find entries in O(1) time.
            foreach ($entry in $archive.Entries) {
                [void]$entries.Add($entry)
                if ($entryLookup.ContainsKey($entry.FullName)) {
                    [void]$duplicateEntryNames.Add($entry.FullName)
                } else {
                    $entryLookup.Add($entry.FullName, $entry)
                }
            }
            $sourceArchive = [pscustomobject]@{
                Archive = $archive
                Entries = $entries
                EntryLookup = $entryLookup
                DuplicateEntryNames = $duplicateEntryNames
            }
            $script:sourceArchiveCache[$Path] = $sourceArchive
            return $sourceArchive
        } catch {
            $cause = $_.Exception.GetBaseException()
            if ($null -ne $archive) { $archive.Dispose() }
            if ($cause -isnot [System.IO.IOException] -or $attempt -eq $ReadAttempts) {
                throw "Could not open source archive '$Path' after $attempt attempt(s). $($cause.Message) If this is a OneDrive file, ensure OneDrive is running and select 'Always keep on this device', then wait for downloading to finish and rerun."
            }
            Write-Warning "Opening source archive failed for '$Path' (attempt $attempt/$ReadAttempts): $($cause.Message) Retrying in $RetryDelaySeconds seconds."
            Start-Sleep -Seconds $RetryDelaySeconds
        }
    }
}

function Close-SourceArchive {
    param([string]$Path)

    if ($script:sourceArchiveCache.ContainsKey($Path)) {
        $script:sourceArchiveCache[$Path].Archive.Dispose()
        [void]$script:sourceArchiveCache.Remove($Path)
    }
}

function Close-AllSourceArchives {
    foreach ($archivePath in @($script:sourceArchiveCache.Keys)) {
        Close-SourceArchive -Path $archivePath
    }
}

function Copy-SourceCandidateWithRetry {
    param($Candidate, [string]$Destination)

    if ($Candidate.SourceKind -eq 'File') {
        Copy-FileWithRetry -Source $Candidate.FilePath -Destination $Destination
        return
    }

    for ($attempt = 1; $attempt -le $ReadAttempts; $attempt++) {
        try {
            $sourceArchive = Get-SourceArchiveWithRetry -Path $Candidate.ArchivePath
            if ($sourceArchive.DuplicateEntryNames.Contains($Candidate.EntryFullName)) {
                throw [System.IO.InvalidDataException]::new(
                    "Duplicate '$($Candidate.EntryFullName)' entries exist in '$($Candidate.ArchivePath)'."
                )
            }
            if (-not $sourceArchive.EntryLookup.ContainsKey($Candidate.EntryFullName)) {
                throw [System.IO.InvalidDataException]::new(
                    "Entry '$($Candidate.EntryFullName)' no longer exists in '$($Candidate.ArchivePath)'."
                )
            }

            $sourceStream = $sourceArchive.EntryLookup[$Candidate.EntryFullName].Open()
            try {
                $destinationStream = [System.IO.File]::Open(
                    $Destination,
                    [System.IO.FileMode]::Create,
                    [System.IO.FileAccess]::Write,
                    [System.IO.FileShare]::None
                )
                try { $sourceStream.CopyTo($destinationStream) }
                finally { $destinationStream.Dispose() }
            } finally { $sourceStream.Dispose() }
            return
        } catch {
            $cause = $_.Exception.GetBaseException()
            Close-SourceArchive -Path $Candidate.ArchivePath
            if ($cause -isnot [System.IO.IOException] -or $attempt -eq $ReadAttempts) {
                throw "Could not read '$($Candidate.EntryFullName)' from '$($Candidate.ArchivePath)' after $attempt attempt(s). $($cause.Message)"
            }
            Write-Warning "Reading '$($Candidate.EntryFullName)' from '$($Candidate.ArchivePath)' failed (attempt $attempt/$ReadAttempts): $($cause.Message) Retrying in $RetryDelaySeconds seconds."
            Start-Sleep -Seconds $RetryDelaySeconds
        }
    }
}

function ConvertTo-SafeRelativeEntryName {
    param([string]$RelativePath, [string]$SourceDescription)

    $normalized = $RelativePath.Replace('\', '/')
    $segments = @($normalized.Split('/'))
    $unsafeSegments = @($segments | Where-Object {
        ($_ -in @('', '.', '..')) -or ([string]$_).Contains(':')
    })
    if ([string]::IsNullOrWhiteSpace($normalized) -or
        $normalized.StartsWith('/') -or
        $unsafeSegments.Count -gt 0) {
        throw "Unsafe source archive entry path '$RelativePath' in '$SourceDescription'."
    }
    return $normalized
}

function New-FileSourceCandidate {
    param([System.IO.FileInfo]$File)

    return [pscustomobject]@{
        SourceKind = 'File'
        FilePath = $File.FullName
        ArchivePath = $null
        EntryFullName = $null
        FullName = $File.FullName
        LastWriteTimeUtc = $File.LastWriteTimeUtc
    }
}

function New-ZipEntrySourceCandidate {
    param([string]$ArchivePath, $Entry)

    return [pscustomobject]@{
        SourceKind = 'ZipEntry'
        FilePath = $null
        ArchivePath = $ArchivePath
        EntryFullName = $Entry.FullName
        FullName = "$ArchivePath::$($Entry.FullName)"
        LastWriteTimeUtc = $Entry.LastWriteTime.UtcDateTime
    }
}

function Add-SourceCandidate {
    param(
        [hashtable]$WorkByZip,
        [string]$TargetName,
        [string]$EntryName,
        $Candidate,
        [AllowNull()][string]$DateFolderName
    )

    $previousCandidates = @()
    if ($WorkByZip[$TargetName].ContainsKey($EntryName)) {
        $previousCandidates = @($WorkByZip[$TargetName][$EntryName].Candidates)
    }
    $WorkByZip[$TargetName][$EntryName] = [pscustomobject]@{
        DateFolderName = $DateFolderName
        Candidates = @($previousCandidates) + @($Candidate)
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
$script:sourceArchiveCache = @{}

try {

$dateFolders = @(Get-ChildItem -LiteralPath $sourceFullPath -Directory |
    Where-Object {
        $_.Name -like $DateFolderPattern -and
        $_.FullName -ine $outputFullPath -and
        $RootFolderNames -inotcontains $_.Name
    } |
    Sort-Object Name)

$dateArchives = @(Get-ChildItem -LiteralPath $sourceFullPath -File -Filter '*.zip' |
    Where-Object {
        $_.BaseName -match '^\d{4}-\d{2}-\d{2}$' -and
        $_.BaseName -like $DateFolderPattern
    } |
    Sort-Object BaseName)

# Process sources chronologically. If an extracted folder and its archive both
# exist, scan the folder first so the archive is the preferred candidate.
$dateSources = @(@(
    foreach ($dateFolder in $dateFolders) {
        [pscustomobject]@{
            DateFolderName = $dateFolder.Name
            SourceKind = 'Folder'
            SourcePriority = 0
            Path = $dateFolder.FullName
        }
    }
    foreach ($dateArchive in $dateArchives) {
        [pscustomobject]@{
            DateFolderName = $dateArchive.BaseName
            SourceKind = 'Archive'
            SourcePriority = 1
            Path = $dateArchive.FullName
        }
    }
) | Sort-Object DateFolderName, SourcePriority)

$fileSearchOption = if ($Recurse) {
    [System.IO.SearchOption]::AllDirectories
} else {
    [System.IO.SearchOption]::TopDirectoryOnly
}

$workByZip = @{}
$foldersScannedByZip = @{}
$subFolderLookup = @{}
$matchingSubFoldersScanned = 0

foreach ($subFolderName in @($SubFolderNames) + @($RootFolderNames)) {
    $workByZip[$subFolderName] = @{}
    $foldersScannedByZip[$subFolderName] = 0
}
foreach ($subFolderName in $SubFolderNames) {
    $subFolderLookup[$subFolderName] = $subFolderName
}

foreach ($dateSource in $dateSources) {
    if ($dateSource.SourceKind -eq 'Folder') {
        # Some extracted imports contain an extra same-named date directory,
        # for example 2026-09-10\2026-09-10\Application.
        $dateSearchRoots = @($dateSource.Path)
        $nestedDatePath = Join-Path -Path $dateSource.Path -ChildPath $dateSource.DateFolderName
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
                $sourceFiles = @([System.IO.Directory]::EnumerateFiles(
                    $subFolderPath,
                    '*',
                    $fileSearchOption
                ) | Where-Object { [System.IO.Path]::GetExtension($_) -in @('.json', '.xml') } | Sort-Object)

                foreach ($sourceFilePath in $sourceFiles) {
                    $sourceFile = Get-Item -LiteralPath $sourceFilePath
                    $relativePath = Get-RelativePath -BasePath $subFolderPath -TargetPath $sourceFile.FullName
                    $entryName = ConvertTo-ZipEntryName -RelativePath $relativePath
                    Add-SourceCandidate -WorkByZip $workByZip -TargetName $subFolderName `
                        -EntryName $entryName -Candidate (New-FileSourceCandidate -File $sourceFile) `
                        -DateFolderName $dateSource.DateFolderName
                }
            }
        }
        continue
    }

    $sourceArchive = Get-SourceArchiveWithRetry -Path $dateSource.Path
    $categoriesFound = @{}
    $logicalEntryNames = @{}
    foreach ($sourceEntry in $sourceArchive.Entries) {
        $sourceEntryName = $sourceEntry.FullName.Replace('\', '/')
        $segments = @($sourceEntryName.Split('/'))
        if ($segments.Count -eq 0) { continue }

        $categoryIndex = 0
        if ($segments[0] -ieq $dateSource.DateFolderName) {
            $categoryIndex = 1
            if ($segments.Count -gt 1 -and $segments[1] -ieq $dateSource.DateFolderName) {
                $categoryIndex = 2
            }
        }
        if ($categoryIndex -ge $segments.Count) { continue }
        $categoryName = $segments[$categoryIndex]
        if (-not $subFolderLookup.ContainsKey($categoryName)) { continue }

        $subFolderName = $subFolderLookup[$categoryName]
        $categoriesFound[$subFolderName] = $true
        if ($sourceEntryName.EndsWith('/') -or $categoryIndex + 1 -ge $segments.Count) { continue }
        $relativePath = ($segments[($categoryIndex + 1)..($segments.Count - 1)] -join '/')
        if ([string]::IsNullOrWhiteSpace($relativePath)) { continue }
        if ([System.IO.Path]::GetExtension($relativePath) -notin @('.json', '.xml')) { continue }
        if ($sourceArchive.DuplicateEntryNames.Contains($sourceEntry.FullName)) {
            throw "Duplicate source archive entry '$($sourceEntry.FullName)' in '$($dateSource.Path)'."
        }
        $entryName = ConvertTo-SafeRelativeEntryName -RelativePath $relativePath -SourceDescription $dateSource.Path
        $logicalEntryKey = $subFolderName + [char]0 + $entryName
        if ($logicalEntryNames.ContainsKey($logicalEntryKey)) {
            throw "Duplicate source archive mapping for '$subFolderName/$entryName' in '$($dateSource.Path)'."
        }
        $logicalEntryNames[$logicalEntryKey] = $true
        Add-SourceCandidate -WorkByZip $workByZip -TargetName $subFolderName `
            -EntryName $entryName -Candidate (New-ZipEntrySourceCandidate -ArchivePath $dateSource.Path -Entry $sourceEntry) `
            -DateFolderName $dateSource.DateFolderName
    }
    foreach ($subFolderName in $categoriesFound.Keys) {
        $matchingSubFoldersScanned++
        $foldersScannedByZip[$subFolderName]++
    }
}

foreach ($rootFolderName in $RootFolderNames) {
    $rootFolderPath = Join-Path -Path $sourceFullPath -ChildPath $rootFolderName
    if (Test-Path -LiteralPath $rootFolderPath -PathType Container) {
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
            Add-SourceCandidate -WorkByZip $workByZip -TargetName $rootFolderName `
                -EntryName $entryName -Candidate (New-FileSourceCandidate -File $sourceFile) -DateFolderName $null
        }
    }

    $rootArchivePath = Join-Path -Path $sourceFullPath -ChildPath ($rootFolderName + '.zip')
    if (-not (Test-Path -LiteralPath $rootArchivePath -PathType Leaf)) { continue }

    $matchingSubFoldersScanned++
    $foldersScannedByZip[$rootFolderName]++
    $sourceArchive = Get-SourceArchiveWithRetry -Path $rootArchivePath
    $logicalEntryNames = @{}
    $rootPrefix = $rootFolderName + '/'
    foreach ($sourceEntry in $sourceArchive.Entries) {
        $sourceEntryName = $sourceEntry.FullName.Replace('\', '/')
        if ($sourceEntryName.EndsWith('/')) { continue }
        if ($sourceEntryName.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            $relativePath = $sourceEntryName.Substring($rootPrefix.Length)
        } elseif (-not $sourceEntryName.Contains('/')) {
            $relativePath = $sourceEntryName
        } else {
            continue
        }
        if ([System.IO.Path]::GetExtension($relativePath) -notin @('.json', '.xml')) { continue }
        if ($sourceArchive.DuplicateEntryNames.Contains($sourceEntry.FullName)) {
            throw "Duplicate source archive entry '$($sourceEntry.FullName)' in '$rootArchivePath'."
        }
        $entryName = ConvertTo-SafeRelativeEntryName -RelativePath $relativePath -SourceDescription $rootArchivePath
        if ($logicalEntryNames.ContainsKey($entryName)) {
            throw "Duplicate source archive mapping for '$rootFolderName/$entryName' in '$rootArchivePath'."
        }
        $logicalEntryNames[$entryName] = $true
        Add-SourceCandidate -WorkByZip $workByZip -TargetName $rootFolderName `
            -EntryName $entryName -Candidate (New-ZipEntrySourceCandidate -ArchivePath $rootArchivePath -Entry $sourceEntry) `
            -DateFolderName $null
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
    DateArchivesScanned = $dateArchives.Count
    DateSourcesScanned = $dateSources.Count
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
$dateEntryPrefixes = @($dateSources | ForEach-Object { $_.DateFolderName + '/' } | Sort-Object -Unique)

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
                            Copy-SourceCandidateWithRetry -Candidate $sourceCandidate -Destination $stagedFilePath
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
} finally {
    Close-AllSourceArchives
}

