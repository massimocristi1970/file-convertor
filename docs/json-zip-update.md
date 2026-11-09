# Updating JSON zip files

Use `scripts\Update-JsonZips.ps1` to scan daily source ZIPs directly, then create or update one consolidated ZIP for each expected report category. Daily archives do not need to be extracted first. Previously extracted dated folders remain supported for compatibility.

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

Example daily source archive:

```text
US_Application_Data\2026-11-06.zip
  2026-11-06\Application\file.json
  2026-11-06\OpenBanking\bank-report.json
  2026-11-06\OneScoreAccept\score-report.json
```

Example zip contents:

```text
_json_zips\Applications.zip
  file.json
  another-file.json
```

The consolidated ZIP files do not contain dated folders. If the same report path exists in multiple daily archives or dated folders, the source from the latest date wins. If both an archive and an extracted folder exist for the same date and path, the archive wins.

The root-level `onescore_missing.zip` and `openbanking_missing.zip` source archives are also read directly. Their category prefix is removed in the consolidated output.

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
- Source archives are opened read-only and report entries are streamed directly into the existing staging workflow. They are not extracted into permanent folders.
- Each zip is built in local temporary storage and published only when complete. A failed source read leaves that zip unchanged; zips completed earlier in the run remain updated. Temporary storage needs room for one archive and one source file, and the output drive needs room for a second copy of the archive during publication.

- The eight default zip files are created if they do not already exist.
- The source folder Application (singular) is written to Applications.zip.
- Daily ZIPs may contain category folders beneath one or two same-named date prefixes; both layouts are scanned.
- Extracted dated folders may place category folders directly beneath the date folder or beneath an extra same-named date folder; both layouts remain supported.
- The root-level `onescore_missing.zip` and `openbanking_missing.zip` archives are consolidated separately, including their JSON and XML reports.
- New source files are added.
- Changed source files are detected by content and replaced in the consolidated ZIP.
- Unchanged source files are skipped.
- Old date-folder entries created by the earlier version of this script are removed from the target zips.
- ZIP entries nested below a report category are always included. For legacy extracted folders, `-Recurse` includes JSON and XML files in nested folders below each named subfolder.
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
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\Test-JsonZipArchiveInput.ps1
```

These use temporary fixtures to check source discovery, extracted-folder compatibility, direct archive ingestion, precedence, unsafe paths, content updates, and locked sources. They also verify that an existing consolidated archive is preserved when an input read fails.

