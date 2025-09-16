@echo off
setlocal EnableDelayedExpansion

REM Assumptions:
REM - Your database CLI program is an executable named 'db_cli.exe' in the current directory.
REM - It reads from stdin and writes to stdout/stderr.
REM - It supports multi-line input ending with ';'.
REM - It exits on 'exit;'.
REM - Persistence files are in the current directory (e.g., *.db, wal.log, catalog.dat).
REM - Adjust DB_COMMAND and file extensions as needed.
REM - Windows environment; tested with cmd.exe.

set DB_COMMAND=wesql.exe
set TIMEOUT=5
set DATA_DIR=.

REM Function to run DB CLI with input and check output/errors
:run_db
set "COMMANDS=%1"
set "EXPECT_OUTPUT=%2"
set "EXPECT_ERROR=%3"
set "CLEANUP=%4"

REM Clean up persistence files if CLEANUP is true
if "%CLEANUP%"=="true" (
    del /Q "%DATA_DIR%\*.db" "%DATA_DIR%\wal.log" "%DATA_DIR%\catalog.dat" 2>nul
)

REM Create temporary input and output files
set "INPUT_FILE=%TEMP%\db_input_%RANDOM%.txt"
set "OUTPUT_FILE=%TEMP%\db_output_%RANDOM%.txt"
set "ERROR_FILE=%TEMP%\db_error_%RANDOM%.txt"

REM Prepare input: Commands + exit
echo %COMMANDS:;=^&echo.;% > "%INPUT_FILE%"
echo exit; >> "%INPUT_FILE%"

REM Run the command, redirect input/output, and enforce timeout
type "%INPUT_FILE%" | %DB_COMMAND% > "%OUTPUT_FILE%" 2> "%ERROR_FILE%"
if %ERRORLEVEL% neq 0 (
    echo Process exited with code %ERRORLEVEL%
    type "%OUTPUT_FILE%"
    type "%ERROR_FILE%"
    exit /b 1
)

REM Check for timeout (simplified; Windows timeout isn't as precise)
timeout /t %TIMEOUT% /nobreak >nul

REM Read output and error
set "OUTPUT="
for /f "delims=" %%i in (%OUTPUT_FILE%) do set "OUTPUT=!OUTPUT! %%i"
set "ERROR="
for /f "delims=" %%i in (%ERROR_FILE%) do set "ERROR=!ERROR! %%i"

REM Check expected output
if not "%EXPECT_OUTPUT%"=="" (
    echo %OUTPUT% | findstr /C:"%EXPECT_OUTPUT%" >nul
    if !ERRORLEVEL! neq 0 (
        echo Test failed: Expected '%EXPECT_OUTPUT%' in output but got:
        type "%OUTPUT_FILE%"
        type "%ERROR_FILE%"
        exit /b 1
    )
)

REM Check expected error
if not "%EXPECT_ERROR%"=="" (
    echo %OUTPUT% %ERROR% | findstr /C:"%EXPECT_ERROR%" >nul
    if !ERRORLEVEL! neq 0 (
        echo Test failed: Expected '%EXPECT_ERROR%' in error/output but got:
        type "%OUTPUT_FILE%"
        type "%ERROR_FILE%"
        exit /b 1
    )
)

REM Clean up temp files
del "%INPUT_FILE%" "%OUTPUT_FILE%" "%ERROR_FILE%"
exit /b 0

REM Test basic CRUD operations
echo Testing basic CRUD operations...
call :run_db "CREATE TABLE test_table (id INT, name VARCHAR);^&echo.INSERT INTO test_table VALUES (1, 'Alice');^&echo.INSERT INTO test_table VALUES (2, 'Bob');^&echo.SELECT * FROM test_table;^&echo.DELETE FROM test_table WHERE id = 1;^&echo.SELECT * FROM test_table;" "2 | Bob" ""
if %ERRORLEVEL%==0 (
    echo Basic CRUD test passed.
) else (
    exit /b 1
)

REM Test persistence
echo Testing persistence...
call :run_db "CREATE TABLE persist_table (id INT);^&echo.INSERT INTO persist_table VALUES (42);" "Inserted" ""
call :run_db "SELECT * FROM persist_table;" "42" ""
if %ERRORLEVEL%==0 (
    echo Persistence test passed.
) else (
    exit /b 1
)

REM Test transactions
echo Testing transactions...
call :run_db "CREATE TABLE tx_table (id INT);^&echo.BEGIN TRANSACTION;^&echo.INSERT INTO tx_table VALUES (1);^&echo.COMMIT;^&echo.SELECT * FROM tx_table;^&echo.BEGIN TRANSACTION;^&echo.INSERT INTO tx_table VALUES (2);^&echo.ROLLBACK;^&echo.SELECT * FROM tx_table;" "1" ""
if %ERRORLEVEL%==0 (
    echo Transactions test passed.
) else (
    exit /b 1
)

REM Test error handling
echo Testing error handling...
call :run_db "CREATE TABLE error_table (id INT);^&echo.INSERT INTO error_table VALUES ('invalid');^&echo.SELECT * FROM non_existent_table;^&echo.Syntax error here" "" "Type mismatch"
if %ERRORLEVEL%==0 (
    echo Error handling test passed.
) else (
    exit /b 1
)

REM Test recovery (simplified; no real crash simulation in batch)
echo Testing crash recovery (simulated)...
echo Testing recovery is limited in batch; assuming WAL rollback. Checking no data after uncommitted insert.
call :run_db "CREATE TABLE recovery_table (id INT);^&echo.BEGIN TRANSACTION;^&echo.INSERT INTO recovery_table VALUES (99);" "" ""
call :run_db "SELECT * FROM recovery_table;" "" "no rows"
if %ERRORLEVEL%==0 (
    echo Recovery test passed (assuming rollback).
) else (
    exit /b 1
)

REM Cleanup before exit
call :run_db "" "" "" true

echo All tests passed!
endlocal