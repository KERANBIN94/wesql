-- 基本表创建测试
CREATE TABLE Users (
    id INTEGER,
    name TEXT,
    age INTEGER
);

-- 插入数据测试
INSERT INTO Users VALUES 
(1, '张三', 25),
(2, '李四', 30),
(3, '王五', 22),
(4, '赵六', 28),
(5, '钱七', 35);

-- 查询测试
SELECT * FROM Users;
SELECT name, age FROM Users WHERE age > 25;
SELECT id, name FROM Users WHERE name LIKE '张%';

-- 更新测试
UPDATE Users SET age = 26 WHERE name = '张三';
UPDATE Users SET name = '张大三' WHERE id = 1;

-- 删除测试
DELETE FROM Users WHERE age < 28;

-- 索引操作测试
CREATE INDEX idx_users_age ON Users(age);
DROP INDEX idx_users_age;

-- 表操作测试
CREATE TABLE Products (
    product_id INTEGER,
    product_name TEXT,
    price INTEGER
);

DROP TABLE Products;

-- 事务测试
BEGIN;
INSERT INTO Users (id, name, age) VALUES (6, '孙八', 29);
COMMIT;

-- 真空操作测试
VACUUM;

-- 复杂WHERE条件测试
SELECT * FROM Users WHERE age > 25 AND age < 35;
SELECT * FROM Users WHERE name = '李四' OR name = '王五';

-- 多条件更新
UPDATE Users SET age = age + 1 WHERE id > 2 AND id < 5;

-- 最终清理
DROP TABLE Users;