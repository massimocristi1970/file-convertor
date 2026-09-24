[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
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

    [switch]$Force
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

$sourceFullPath = Get-UnresolvedFullPath -Path $SourceRoot
if (-not (Test-Path -LiteralPath $sourceFullPath -PathType Container)) {
    throw "SourceRoot does not exist or is not a folder: $sourceFullPath"
}

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
$matchingSubFoldersScanned = 0

foreach ($subFolderName in @($SubFolderNames) + @($RootFolderNames)) {
    $workByZip[$subFolderName] = @{}
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
            $jsonFiles = @([System.IO.Directory]::EnumerateFiles(
                $subFolderPath,
                '*.json',
                $fileSearchOption
            ) | Sort-Object)

            foreach ($jsonFilePath in $jsonFiles) {
                $jsonFile = Get-Item -LiteralPath $jsonFilePath
                $relativeJsonPath = Get-RelativePath -BasePath $subFolderPath -TargetPath $jsonFile.FullName
                $entryName = ConvertTo-ZipEntryName -RelativePath $relativeJsonPath

                # If the same JSON path exists in multiple dated folders, the latest dated folder wins.
                $workByZip[$subFolderName][$entryName] = [pscustomobject]@{
                    File = $jsonFile
                    DateFolderName = $dateFolder.Name
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
    $sourceFiles = @([System.IO.Directory]::EnumerateFiles(
        $rootFolderPath,
        '*',
        $fileSearchOption
    ) | Sort-Object)

    foreach ($sourceFilePath in $sourceFiles) {
        $sourceFile = Get-Item -LiteralPath $sourceFilePath
        $relativePath = Get-RelativePath -BasePath $rootFolderPath -TargetPath $sourceFile.FullName
        $entryName = ConvertTo-ZipEntryName -RelativePath $relativePath
        $workByZip[$rootFolderName][$entryName] = [pscustomobject]@{
            File = $sourceFile
            DateFolderName = $null
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
}
$zipsTouched = @{}
$dateEntryPrefixes = @($dateFolders | ForEach-Object { $_.Name + '/' })

foreach ($subFolderName in @($SubFolderNames) + @($RootFolderNames)) {
    $entriesToWrite = $workByZip[$subFolderName]
    $zipBaseName = if ($subFolderName -ieq 'Application') { 'Applications' } else { $subFolderName }
    $zipPath = Join-Path -Path $outputFullPath -ChildPath ($zipBaseName + '.zip')
    $zipExists = Test-Path -LiteralPath $zipPath -PathType Leaf

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

                    foreach ($entryName in ($entriesToWrite.Keys | Sort-Object)) {
                        $jsonFile = $entriesToWrite[$entryName].File
                        $entry = $zip.GetEntry($entryName)

                        $shouldWrite = $Force -or $null -eq $entry
                        if (-not $shouldWrite) {
                            $entryLastWriteUtc = $entry.LastWriteTime.UtcDateTime
                            $sourceIsNewer = ($jsonFile.LastWriteTimeUtc - $entryLastWriteUtc).TotalSeconds -gt 2
                            $shouldWrite = $sourceIsNewer -or $jsonFile.Length -ne $entry.Length
                        }

                        if (-not $shouldWrite) {
                            $summary.FilesSkipped++
                            continue
                        }

                        # Fully download/read the source before deleting its previous entry.
                        Copy-FileWithRetry -Source $jsonFile.FullName -Destination $stagedFilePath

                        if ($null -ne $entry) {
                            $entry.Delete()
                            $summary.FilesUpdated++
                        } else {
                            $summary.FilesAdded++
                        }

                        [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                            $zip,
                            $stagedFilePath,
                            $entryName,
                            [System.IO.Compression.CompressionLevel]::Optimal
                        ) | Out-Null

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

