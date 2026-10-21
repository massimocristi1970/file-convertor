function Resolve-JsonSourceRoot {
    param([string]$SourceRoot)

    if (-not [string]::IsNullOrWhiteSpace($SourceRoot)) {
        $sourceFullPath = [System.IO.Path]::GetFullPath(
            $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($SourceRoot)
        )
        if (-not (Test-Path -LiteralPath $sourceFullPath -PathType Container)) {
            throw "SourceRoot does not exist or is not a folder: $sourceFullPath"
        }
        return $sourceFullPath
    }

    $relativeSourcePath = 'US Product\US_Application_Data'
    $oneDriveFolderName = 'OneDrive - Savvy Loan Products Ltd'
    $oneDriveRoots = @(
        foreach ($variableName in @('OneDriveCommercial', 'OneDrive', 'OneDriveConsumer')) {
            $value = [System.Environment]::GetEnvironmentVariable($variableName)
            if (-not [string]::IsNullOrWhiteSpace($value)) { $value }
        }

        # OneDrive may have several business accounts or no environment variable.
        $accountsPath = 'HKCU:\Software\Microsoft\OneDrive\Accounts'
        foreach ($account in @(Get-ChildItem -LiteralPath $accountsPath -ErrorAction SilentlyContinue)) {
            $settings = Get-ItemProperty -LiteralPath $account.PSPath -Name UserFolder -ErrorAction SilentlyContinue
            if ($null -ne $settings -and -not [string]::IsNullOrWhiteSpace($settings.UserFolder)) {
                $settings.UserFolder
            }
        }

        if (-not [string]::IsNullOrWhiteSpace($env:USERPROFILE)) {
            [System.IO.Path]::Combine($env:USERPROFILE, $oneDriveFolderName)
        }
    )

    $candidates = @(
        foreach ($oneDriveRoot in $oneDriveRoots) {
            [System.IO.Path]::Combine($oneDriveRoot, $relativeSourcePath)
        }

        # Check known locations only; never recursively search whole drives.
        foreach ($drive in @(Get-PSDrive -PSProvider FileSystem)) {
            Join-Path $drive.Root (Join-Path $oneDriveFolderName $relativeSourcePath)
            Join-Path $drive.Root $relativeSourcePath
            Join-Path $drive.Root 'US_Application_Data'

            # Also handle a sync folder moved to a different drive with a stale
            # OneDrive setting, preserving its profile/folder layout.
            foreach ($oneDriveRoot in $oneDriveRoots) {
                if ($oneDriveRoot -match '^[A-Za-z]:[\\/]') {
                    $rebasedRoot = [System.IO.Path]::Combine($drive.Root, $oneDriveRoot.Substring(3))
                    [System.IO.Path]::Combine($rebasedRoot, $relativeSourcePath)
                }
            }
        }
    )

    $checkedPaths = @{}
    $matches = @(
        foreach ($candidate in $candidates) {
            $fullPath = [System.IO.Path]::GetFullPath($candidate).TrimEnd('\')
            if ($checkedPaths.ContainsKey($fullPath)) { continue }
            $checkedPaths[$fullPath] = $true
            if ([System.IO.Directory]::Exists($fullPath)) { $fullPath }
        }
    )

    if ($matches.Count -eq 1) { return $matches[0] }
    if ($matches.Count -gt 1) {
        throw ("Multiple source folders were found. Choose one with -SourceRoot, or pass it as the first argument to run_json_zip_update.bat:`n" + ($matches -join "`n"))
    }
    $checkedPathsText = ($checkedPaths.Keys | Sort-Object) -join "`n"
    throw "Could not find '$relativeSourcePath' in the configured OneDrive folders or known drive locations. Pass the source folder as the first argument to run_json_zip_update.bat, or use -SourceRoot with Update-JsonZips.ps1. Checked:`n$checkedPathsText"
}
