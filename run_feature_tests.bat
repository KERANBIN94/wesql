@echo off
setlocal

REM Assuming the executable is in the 'build' directory
set "WESQL_EXEC=build\wesql.exe"

REM Check if the executable exists
if not exist %WESQL_EXEC% (
    echo Error: wesql.exe not found in build directory.
    echo Please build the project first using CMake and Make.
    exit /b 1
)

echo --- Running wesql Feature Tests ---
echo.

REM === Test 1: DDL & DML Operations ===
echo [TEST] Creating table, inserting data, and selecting...
(
    echo CREATE TABLE products (id INT, name VARCHAR(50), price INT);
    echo INSERT INTO products VALUES (1, 'Laptop', 1200);
    echo INSERT INTO products VALUES (2, 'Mouse', 25);
    echo INSERT INTO products VALUES (3, 'Keyboard', 75);
    echo SELECT * FROM products;
) | %WESQL_EXEC%
echo.

REM === Test 2: Transaction COMMIT ===
echo [TEST] Transaction COMMIT...
echo --- Before COMMIT, inserting 'Monitor'...
(
    echo BEGIN;
    echo INSERT INTO products VALUES (4, 'Monitor', 300);
    echo COMMIT;
) | %WESQL_EXEC%

echo --- After COMMIT, selecting all to verify 'Monitor' is present...
(
    echo SELECT * FROM products;
) | %WESQL_EXEC%
echo.

REM === Test 3: Transaction ROLLBACK ===
echo [TEST] Transaction ROLLBACK...
echo --- Before ROLLBACK, attempting to insert 'Webcam'...
(
    echo BEGIN;
    echo INSERT INTO products VALUES (5, 'Webcam', 80);
    echo ROLLBACK;
) | %WESQL_EXEC%

echo --- After ROLLBACK, selecting all to verify 'Webcam' is NOT present...
(
    echo SELECT * FROM products;
) | %WESQL_EXEC%
echo.

REM === Test 4: Cleanup ===
echo [TEST] Cleaning up the 'products' table...
(
    echo DROP TABLE products;
) | %WESQL_EXEC%
echo.

echo --- All Tests Finished ---

endlocal
