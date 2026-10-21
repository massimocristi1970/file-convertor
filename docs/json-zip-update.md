# Updating JSON zip files

Use `scripts\Update-JsonZips.ps1` to scan a source folder that contains dated folders, then create or update one zip file for each expected JSON subfolder.

The source folder is detected on each machine, so the same launcher works whether the files live on `C:`, `D:`, or another drive, and whether the Windows user name changes.

## Default command

Double-click `run_json_zip_update.bat`, or run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Update-JsonZips.ps1 -Recurse
```

Automatic detection checks `US Product\US_Application_Data` beneath the current machine's OneDrive folders, using its OneDrive environment variables and registered accounts. It also checks known locations on available filesystem drives: `OneDrive - Savvy Loan Products Ltd\US Product\US_Application_Data`, `US Product\US_Application_Data`, and `US_Application_Data`, plus the configured OneDrive folder layout on each drive. These are direct folder checks; the script does not search entire drives recursively.

The script prints the resolved source folder before processing. If no source is found, or several different folders match, supply the intended folder explicitly. An explicit path always takes precedence and must exist.

By default, zips are written under:

```text
<SourceRoot>\_json_zips
```

The default zip files are:

```text
<SourceRoot>\_json_zips\Applications.zip
<SourceRoot>\_json_zips\EssentialsJsonAccept.zip
<SourceRoot>\_json_zips\EssentialsJsonReject.zip
<SourceRoot>\_json_zips\OpenBanking.zip
<SourceRoot>\_json_zips\OneScoreAccept.zip
<SourceRoot>\_json_zips\OneScoreReject.zip
<SourceRoot>\_json_zips\onescore_missing.zip
<SourceRoot>\_json_zips\openbanking_missing.zip
```

Example source:

```text
US_Application_Data\2026-07-11\Application\file.json
US_Application_Data\2026-07-12\Application\another-file.json
```

Example zip contents:

```text
_json_zips\Applications.zip
  file.json
  another-file.json
```

The zip files do not contain dated folders. If the same JSON path exists in multiple dated folders, the file from the latest dated folder name wins.

## Different drive or output folder

To use a source outside the detected locations, pass it to the launcher:

```powershell
.\run_json_zip_update.bat "D:\US Product\US_Application_Data"
```

Or supply a source and optional output folder directly:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Update-JsonZips.ps1 -SourceRoot "D:\US Product\US_Application_Data" -OutputRoot "D:\US Product\US_Application_Data_Zips" -Recurse
```

## Preview changes

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Update-JsonZips.ps1 -Recurse -WhatIf
```

## Notes

- Cloud reads are retried up to three times, with five seconds between attempts. Use `-ReadAttempts` and `-RetryDelaySeconds` to adjust this.
- Each zip is built in local temporary storage and published only when complete. A failed source read leaves that zip unchanged; zips completed earlier in the run remain updated. Temporary storage needs room for one archive and one source file, and the output drive needs room for a second copy of the archive during publication.

- The eight default zip files are created if they do not already exist.
- The source folder Application (singular) is written to Applications.zip.
- Dated imports may place category folders directly beneath the date folder or beneath an extra same-named date folder; both layouts are scanned.
- The root-level onescore_missing and openbanking_missing folders are zipped separately, including their XML files.
- New source files are added.
- Changed source files are replaced in the zip when their timestamp or file size differs.
- Unchanged source files are skipped.
- Old date-folder entries created by the earlier version of this script are removed from the target zips.
- `-Recurse` includes JSON files in nested folders below each named subfolder. Omit it if each named subfolder only has JSON files directly inside it.
- Use `-DateFolderPattern` if you want to limit which dated folders are scanned, for example `-DateFolderPattern "2026-*"`.
- Use `-SubFolderNames` to override the default list of zip targets.

## OneDrive timeout

If you see "The cloud operation was not completed before the time-out period expired", OneDrive could not supply a file in time. The script reports the failing path and retries before stopping.

In File Explorer, right-click the source folder and select **Always keep on this device**. Ensure OneDrive is running and connected, wait for downloading to finish, then rerun `run_json_zip_update.bat`. Microsoft documents this option in [Files On-Demand for Windows](https://support.microsoft.com/en-US/onedrive/save-disk-space-with-onedrive-files-on-demand-for-windows).

If an earlier version of the script failed, rerun once with `-Force` to replace any incomplete entries left by that run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Update-JsonZips.ps1 -Recurse -Force
```

Run the local regression checks:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\Test-JsonSourceRoot.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\Test-JsonZipUpdate.ps1
```

These use temporary fixtures to check source discovery and zip updates, including a locked source file to verify retries and preservation of the existing archive on failure.

