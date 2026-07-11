@echo off
SETLOCAL

REM Change directory to where this .bat file lives
cd /d "%~dp0"

REM Default source folder for this machine. Pass a different folder as the first argument on another machine.
SET "SOURCE_ROOT=C:\Users\Massimo Cristi\OneDrive - Savvy Loan Products Ltd\US Product\US_Application_Data"
IF NOT "%~1"=="" SET "SOURCE_ROOT=%~1"

echo Updating JSON zip files from:
echo %SOURCE_ROOT%
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\Update-JsonZips.ps1" -SourceRoot "%SOURCE_ROOT%" -Recurse

SET "EXIT_CODE=%ERRORLEVEL%"
echo.
IF "%EXIT_CODE%"=="0" (
    echo Done.
) ELSE (
    echo Failed with exit code %EXIT_CODE%.
)

ENDLOCAL
pause
exit /b %EXIT_CODE%
