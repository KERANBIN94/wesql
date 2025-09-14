@echo off

:: Test script for wesql on Windows


:: --- Build the project ---
echo Building the wesql project...

if not exist build mkdir build
cd build

:: Run cmake to generate build files (e.g., for Visual Studio)
cmake ..

:: Build the project using cmake's build tool mode
cmake --build .

cd ..

echo Build complete.


:: --- Test Cases ---

:: Define the wesql executable path
set WESQL_EXEC=build\Debug\wesql.exe


:: Function-like structure for running tests
:run_test
    echo.
    echo --- Testing: %~1 ---
    echo Running SQL:
    echo %~2
    echo --------------------
    (echo %~2) | %WESQL_EXEC%
    echo --- Test End ---
    goto :eof


:: 1. Basic DDL and DML
call :run_test "CREATE TABLE and INSERT" "CREATE TABLE users (id INT, name TEXT); & echo INSERT INTO users VALUES (1, 'Alice'); & echo INSERT INTO users VALUES (2, 'Bob');"


:: 2. Basic SELECT
call :run_test "SELECT all from users" "SELECT * FROM users;"


:: 3. SELECT with WHERE clause
call :run_test "SELECT with WHERE clause" "SELECT name FROM users WHERE id = 2;"


:: 4. CREATE INDEX
call :run_test "CREATE INDEX on users(id)" "CREATE INDEX user_id_idx ON users (id); & echo SELECT * FROM users WHERE id = 1;"


:: 5. Transaction Test: COMMIT
call :run_test "Transaction COMMIT" "BEGIN; & echo INSERT INTO users VALUES (3, 'Charlie'); & echo COMMIT; & echo SELECT * FROM users;"


:: 6. Transaction Test: ROLLBACK
call :run_test "Transaction ROLLBACK" "SELECT * FROM users; & echo BEGIN; & echo INSERT INTO users VALUES (4, 'David'); & echo ROLLBACK; & echo SELECT * FROM users;"



echo.
echo All tests executed.

