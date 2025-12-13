@echo off
title CAN Logger Setup - Auto Clean
echo =====================================================
echo       CAN Logger Environment Auto Setup
echo =====================================================
echo.

chcp 65001 >nul
python -m ensurepip --upgrade >nul 2>&1
python -m pip install --upgrade pip setuptools wheel >nul 2>&1
echo Installing all required libraries...
python -m pip install -r requirements.txt --upgrade --force-reinstall --no-warn-script-location --disable-pip-version-check >nul 2>&1
python -m pip check >nul 2>&1
echo.
echo =====================================================
echo ✅ Environment setup completed successfully!
echo =====================================================
echo.
pause