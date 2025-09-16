@echo off
cd /d "%~dp0"
if not exist "data" mkdir data
build\Debug\wesql.exe
pause