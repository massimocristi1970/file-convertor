# Updating JSON zip files

Use `scripts\Update-JsonZips.ps1` to scan a source folder that contains dated folders, then create or update one zip file for each expected JSON subfolder.

The source path is provided when you run the command, so the same script works whether the files live on `C:`, `D:`, or another local drive.

## Default command

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Update-JsonZips.ps1 -SourceRoot "C:\Users\Massimo Cristi\OneDrive - Savvy Loan Products Ltd\US Product\US_Application_Data" -Recurse
```

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
```

Example source:

```text
US_Application_Data\2026-07-11\Applications\file.json
US_Application_Data\2026-07-12\Applications\another-file.json
```

Example zip contents:

```text
_json_zips\Applications.zip
  file.json
  another-file.json
```

The zip files do not contain dated folders. If the same JSON path exists in multiple dated folders, the file from the latest dated folder name wins.

## Different drive or output folder

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Update-JsonZips.ps1 -SourceRoot "D:\US Product\US_Application_Data" -OutputRoot "D:\US Product\US_Application_Data_Zips" -Recurse
```

## Preview changes

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Update-JsonZips.ps1 -SourceRoot "C:\Users\Massimo Cristi\OneDrive - Savvy Loan Products Ltd\US Product\US_Application_Data" -Recurse -WhatIf
```

## Notes

- The four default zip files are created if they do not already exist.
- New JSON files are added.
- Changed JSON files are replaced in the zip when their timestamp or file size differs.
- Unchanged JSON files are skipped.
- Old date-folder entries created by the earlier version of this script are removed from the target zips.
- `-Recurse` includes JSON files in nested folders below each named subfolder. Omit it if each named subfolder only has JSON files directly inside it.
- Use `-DateFolderPattern` if you want to limit which dated folders are scanned, for example `-DateFolderPattern "2026-*"`.
- Use `-SubFolderNames` to override the default list of zip targets.

