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

    [switch]$Recurse,

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

$dateFolders = Get-ChildItem -LiteralPath $sourceFullPath -Directory |
    Where-Object {
        $_.Name -like $DateFolderPattern -and
        $_.FullName -ine $outputFullPath
    } |
    Sort-Object Name

$fileSearchOption = if ($Recurse) {
    [System.IO.SearchOption]::AllDirectories
} else {
    [System.IO.SearchOption]::TopDirectoryOnly
}

$workByZip = @{}
$matchingSubFoldersScanned = 0

foreach ($subFolderName in $SubFolderNames) {
    $workByZip[$subFolderName] = @{}
}

foreach ($dateFolder in $dateFolders) {
    foreach ($subFolderName in $SubFolderNames) {
        $subFolderPath = Join-Path -Path $dateFolder.FullName -ChildPath $subFolderName
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

foreach ($subFolderName in $SubFolderNames) {
    $entriesToWrite = $workByZip[$subFolderName]
    $zipBaseName = if ($subFolderName -ieq 'Application') { 'Applications' } else { $subFolderName }
    $zipPath = Join-Path -Path $outputFullPath -ChildPath ($zipBaseName + '.zip')
    $zipExists = Test-Path -LiteralPath $zipPath -PathType Leaf

    if ($PSCmdlet.ShouldProcess($zipPath, "Update flat JSON zip for $subFolderName")) {
        if (-not (Test-Path -LiteralPath $outputFullPath -PathType Container)) {
            New-Item -ItemType Directory -Path $outputFullPath | Out-Null
        }

        $zipStream = [System.IO.File]::Open(
            $zipPath,
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

                    if ($null -ne $entry) {
                        $entry.Delete()
                        $summary.FilesUpdated++
                    } else {
                        $summary.FilesAdded++
                    }

                    [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                        $zip,
                        $jsonFile.FullName,
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
    }
}

$summary.ZipsTouched = $zipsTouched.Count
[pscustomobject]$summary

