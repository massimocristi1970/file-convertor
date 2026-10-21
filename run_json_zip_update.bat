@echo off
SETLOCAL

REM Change directory to where this .bat file lives
cd /d "%~dp0"

REM Detect the source on this machine, or use the folder passed as the first argument.
IF "%~1"=="" (
    powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\Update-JsonZips.ps1" -Recurse
) ELSE (
    powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\Update-JsonZips.ps1" -SourceRoot "%~1" -Recurse
)

SET "EXIT_CODE=%ERRORLEVEL%"
echo.
IF "%EXIT_CODE%"=="0" (
    echo Done.
) ELSE (
    echo Failed with exit code %EXIT_CODE%.
)

pause
ENDLOCAL & exit /b %EXIT_CODE%
