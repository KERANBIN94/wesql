@echo off
echo --- Comprehensive Test Suite for wesql ---

:: Define the wesql executable path
set WESQL_EXEC=build\wesql.exe

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

:: =================================
:: 1. Setup & Basic DML
:: =================================

call :run_test "CREATE TABLE departments (with PRIMARY KEY)" "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT NOT NULL);"
call :run_test "CREATE TABLE users (with PRIMARY KEY and FOREIGN KEY)" "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, department_id INT, FOREIGN KEY (department_id) REFERENCES departments(id));"

call :run_test "INSERT into departments" "INSERT INTO departments VALUES (1, 'Engineering'); & echo INSERT INTO departments VALUES (2, 'Marketing');"
call :run_test "INSERT into users" "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 1); & echo INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 2); & echo INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 1); & echo INSERT INTO users VALUES (4, 'David', 'david@example.com', NULL);"

:: =================================
:: 2. Core SELECT operations
:: =================================

call :run_test "SELECT with INNER JOIN" "SELECT users.name, departments.name FROM users JOIN departments ON users.department_id = departments.id;"
call :run_test "SELECT with LEFT JOIN" "SELECT users.name, departments.name FROM users LEFT JOIN departments ON users.department_id = departments.id;"
call :run_test "SELECT with WHERE ... IS NULL" "SELECT name FROM users WHERE department_id IS NULL;"
call :run_test "SELECT with LIKE" "SELECT name FROM users WHERE email LIKE '%@example.com';"
call :run_test "SELECT with BETWEEN" "SELECT name, id FROM users WHERE id BETWEEN 2 AND 4;"
call :run_test "SELECT with IN" "SELECT name, id FROM users WHERE id IN (1, 3);"
call :run_test "SELECT with subquery" "SELECT name FROM users WHERE department_id IN (SELECT id FROM departments WHERE name = 'Engineering');"
call :run_test "SELECT with UNION" "(SELECT name FROM users WHERE department_id = 1) UNION (SELECT name FROM users WHERE department_id = 2);"
call :run_test "SELECT with INTERSECT" "(SELECT name FROM users WHERE department_id = 1) INTERSECT (SELECT name FROM users WHERE id < 3);"
call :run_test "SELECT with EXCEPT" "(SELECT name FROM users WHERE department_id = 1) EXCEPT (SELECT name FROM users WHERE id < 3);"
call :run_test "SELECT with EXISTS" "SELECT name FROM departments WHERE EXISTS (SELECT 1 FROM users WHERE users.department_id = departments.id);"

:: =================================
:: 3. Advanced Querying
:: =================================

call :run_test "SELECT with DISTINCT" "SELECT DISTINCT department_id FROM users;"
call :run_test "AGGREGATE functions (COUNT, AVG)" "SELECT COUNT(*), AVG(id) FROM users;"
call :run_test "GROUP BY and HAVING" "SELECT d.name, COUNT(u.id) FROM departments d JOIN users u ON d.id = u.department_id GROUP BY d.name HAVING COUNT(u.id) > 1;"
call :run_test "ORDER BY (DESC) and LIMIT" "SELECT name, email FROM users ORDER BY name DESC LIMIT 2;"
call :run_test "SELECT with LIMIT and OFFSET" "SELECT id, name FROM users ORDER BY id LIMIT 2 OFFSET 1;"
call :run_test "SELECT with CASE expression" "SELECT name, CASE WHEN department_id = 1 THEN 'Engineer' WHEN department_id = 2 THEN 'Marketer' ELSE 'Unassigned' END AS role FROM users;"

:: =================================
:: 4. Data & Schema Modification
:: =================================

call :run_test "UPDATE data" "UPDATE users SET email = 'bob_new@example.com' WHERE name = 'Bob'; & echo SELECT * FROM users WHERE name = 'Bob';"
call :run_test "DELETE data" "DELETE FROM users WHERE name = 'David'; & echo SELECT * FROM users;"
call :run_test "ALTER TABLE ADD COLUMN" "ALTER TABLE users ADD COLUMN tenure_years INT; & echo INSERT INTO users VALUES (5, 'Eve', 'eve@example.com', 2, 3);"
call :run_test "ALTER TABLE DROP COLUMN" "ALTER TABLE users DROP COLUMN tenure_years; & echo SELECT * FROM users;"

:: =================================
:: 5. Transactions
:: =================================

call :run_test "Transaction COMMIT" "BEGIN; & echo INSERT INTO users (id, name, email, department_id) VALUES (6, 'Frank', 'frank@example.com', 2); & echo COMMIT; & echo SELECT * FROM users;"
call :run_test "Transaction ROLLBACK" "BEGIN; & echo INSERT INTO users (id, name, email, department_id) VALUES (7, 'Grace', 'grace@example.com', 1); & echo ROLLBACK; & echo SELECT * FROM users;"

:: =================================
:: 6. Views and Indexes
:: =================================

call :run_test "CREATE INDEX" "CREATE INDEX user_name_idx ON users (name);"
call :run_test "CREATE VIEW" "CREATE VIEW engineering_users AS SELECT name, email FROM users WHERE department_id = 1;"
call :run_test "SELECT from VIEW" "SELECT * FROM engineering_users;"

:: =================================
:: 7. Cleanup
:: =================================

call :run_test "DROP VIEW" "DROP VIEW engineering_users;"
call :run_test "DROP INDEX" "DROP INDEX user_name_idx;"
call :run_test "DROP TABLE (users)" "DROP TABLE users;"
call :run_test "DROP TABLE (departments)" "DROP TABLE departments;"


echo.
echo All tests executed.
est "ALTER TABLE ADD COLUMN" "ALTER TABLE users ADD COLUMN tenure_years INT; & echo INSERT INTO users VALUES (5, 'Eve', 'eve@example.com', 2, 3);"
call :run_test "ALTER TABLE DROP COLUMN" "ALTER TABLE users DROP COLUMN tenure_years; & echo SELECT * FROM users;"

:: =================================
:: 5. Transactions
:: =================================

call :run_test "Transaction COMMIT" "BEGIN; & echo INSERT INTO users (id, name, email, department_id) VALUES (6, 'Frank', 'frank@example.com', 2); & echo COMMIT; & echo SELECT * FROM users;"
call :run_test "Transaction ROLLBACK" "BEGIN; & echo INSERT INTO users (id, name, email, department_id) VALUES (7, 'Grace', 'grace@example.com', 1); & echo ROLLBACK; & echo SELECT * FROM users;"

:: =================================
:: 6. Views and Indexes
:: =================================

call :run_test "CREATE INDEX" "CREATE INDEX user_name_idx ON users (name);"
call :run_test "CREATE VIEW" "CREATE VIEW engineering_users AS SELECT name, email FROM users WHERE department_id = 1;"
call :run_test "SELECT from VIEW" "SELECT * FROM engineering_users;"

:: =================================
:: 7. Cleanup
:: =================================

call :run_test "DROP VIEW" "DROP VIEW engineering_users;"
call :run_test "DROP INDEX" "DROP INDEX user_name_idx;"
call :run_test "DROP TABLE (users)" "DROP TABLE users;"
call :run_test "DROP TABLE (departments)" "DROP TABLE departments;"


echo.
echo All tests executed.
