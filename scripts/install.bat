@echo off
REM green2blue Windows Installer
REM Double-click this file to install green2blue.
REM
REM What it does:
REM   1. Checks for Python 3.10+
REM   2. Creates a virtual environment at %USERPROFILE%\.green2blue\
REM   3. Installs green2blue with encrypted backup support
REM   4. Creates a run-green2blue.bat launcher on your Desktop
REM   5. Launches the interactive wizard

echo.
echo   green2blue Installer
echo   ====================
echo.

REM --- Check for Python ---
set PYTHON=

REM Try py launcher first (standard Windows Python install)
where py >nul 2>nul
if %errorlevel% equ 0 (
    for /f "tokens=*" %%i in ('py -3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"') do set PY_VERSION=%%i
    for /f "tokens=1,2 delims=." %%a in ("%PY_VERSION%") do (
        if %%a GEQ 3 if %%b GEQ 10 set PYTHON=py -3
    )
)

REM Try python3
if "%PYTHON%"=="" (
    where python3 >nul 2>nul
    if %errorlevel% equ 0 (
        set PYTHON=python3
    )
)

REM Try python
if "%PYTHON%"=="" (
    where python >nul 2>nul
    if %errorlevel% equ 0 (
        for /f "tokens=*" %%i in ('python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"') do set PY_VERSION=%%i
        for /f "tokens=1,2 delims=." %%a in ("%PY_VERSION%") do (
            if %%a GEQ 3 if %%b GEQ 10 set PYTHON=python
        )
    )
)

if "%PYTHON%"=="" (
    echo   Python 3.10+ not found.
    echo.
    echo   Please install Python from: https://www.python.org/downloads/
    echo.
    echo   IMPORTANT: Check "Add Python to PATH" during installation.
    echo.
    echo   After installing Python, double-click this file again.
    echo.
    pause
    exit /b 1
)

echo   Found Python: %PYTHON%
echo.

REM --- Create venv ---
set VENV_DIR=%USERPROFILE%\.green2blue

if exist "%VENV_DIR%" (
    echo   Existing installation found. Updating...
) else (
    echo   Creating virtual environment...
)

%PYTHON% -m venv "%VENV_DIR%" --clear
call "%VENV_DIR%\Scripts\activate.bat"

REM --- Install green2blue ---
echo   Installing green2blue...

REM Check if we're in the repo directory
set SCRIPT_DIR=%~dp0..
if exist "%SCRIPT_DIR%\pyproject.toml" (
    pip install -q "%SCRIPT_DIR%[encrypted]"
) else (
    pip install -q "green2blue[encrypted]"
)

echo   Installation complete!
echo.

REM --- Create Desktop launcher ---
set LAUNCHER=%USERPROFILE%\Desktop\run-green2blue.bat
(
    echo @echo off
    echo call "%VENV_DIR%\Scripts\activate.bat"
    echo green2blue
    echo pause
) > "%LAUNCHER%"

echo   Created launcher: %LAUNCHER%
echo   Double-click it anytime to run green2blue.
echo.

REM --- Launch wizard ---
echo   Launching green2blue...
echo.
green2blue
pause
