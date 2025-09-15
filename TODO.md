### 整体系统架构

系统分为三个主要层：SQL 编译器（用于解析和规划的前端）、存储层（模拟操作系统级页面管理）和数据库核心（执行和集成）。入口点是一个命令行界面 (CLI)，它从用户输入读取 SQL 语句，通过各层处理，并在控制台输出结果或错误。

- **CLI 接口层**
  - 通过命令行处理用户交互。
  - [DONE] 实现主循环，从标准输入读取 SQL 输入（如果需要，支持多行，例如以 ';' 结束）。
  - [DONE] 将输入路由到 SQL 编译器进行处理。
  - [PARTIALLY DONE] 显示执行结果（例如，使用打印语句将查询行以表格格式显示）或错误。
  - [DONE] 处理程序退出（例如，在 "exit" 命令时），并确保关闭时数据持久化。

- **SQL 编译器层**
  - 将 SQL 输入处理成令牌、AST、语义检查和执行计划。
  - **词法分析器**
    - [PARTIALLY DONE] 定义令牌类型：KEYWORD（例如 SELECT、CREATE）、IDENTIFIER（例如表/列名）、CONST（数字、字符串）、OPERATOR（例如 >、=）、DELIMITER（例如 ;、,）。
    - [PARTIALLY DONE] 实现扫描器，读取输入字符串并生成带有类型、词素、行、列的令牌流。
    - [DONE] 处理错误：对于非法字符或无效输入，输出错误类型和位置（例如未闭合字符串）。
    - [DONE] 支持基本 SQL 子集：CREATE TABLE、INSERT、SELECT、DELETE。
  - **语法分析器**
    - [DONE] 使用递归下降、LL(1) 或 LR 解析构建抽象语法树 (AST)。
    - [DONE] 支持语法：CREATE TABLE（带有列和类型，如 INT、VARCHAR）、INSERT INTO VALUES、SELECT FROM WHERE、DELETE FROM WHERE。
    - [DONE] 输出 AST 结构（例如作为树打印或 JSON 用于调试）。
    - [DONE] 处理语法错误：输出错误位置和预期符号。
  - **语义分析器**
    - [DONE] 遍历 AST，使用 Catalog 检查表/列存在性。
    - [DONE] 检查类型一致性（例如 INT 列的 INT 值）。
    - [DONE] 检查列数/顺序（例如 INSERT 值匹配表列）。
    - [PARTIALLY DONE] 维护和更新 Catalog（内存结构，通过存储层持久化）。
    - [DONE] 输出语义结果或带有类型、位置、原因的错误。
  - **执行计划生成器**
    - [DONE] 将 AST 转换为逻辑计划，带有运算符：CreateTable、Insert、SeqScan、Filter、Project。
    - [DONE] 输出计划作为树、JSON 或 S-表达式。
    - [DONE] 对于不支持的功能或缺失信息，使用错误处理。

- **存储层**
  - 使用缓存模拟操作系统页面存储，用于数据持久化。
  - **页面存储管理器**
    - [DONE] 定义页面结构：固定大小（例如 4KB）、唯一 page_id。
    - [PARTIALLY DONE] 实现页面分配：分配新 page_id，将表映射到页面集。
    - [PARTIALLY DONE] 实现页面释放：在删除或表删除时释放页面。
    - [DONE] 模拟磁盘 I/O：使用文件存储页面（例如每个数据库或每个表一个文件）。
    - [DONE] 提供 API：read_page(page_id) -> 字节、write_page(page_id, data)。
    - [PARTIALLY DONE] 处理表-页面映射：维护结构以链接表名到 page_id 列表。
  - **缓存管理器**
    - [DONE] 实现缓存池（例如有限大小的字典）。
    - [DONE] 支持替换策略：LRU（使用 OrderedDict）或 FIFO。
    - [DONE] 跟踪缓存命中/未命中，并记录替换（用于调试）。
    - [DONE] 提供 API：get_page(page_id)（如果未命中，从磁盘加载，应用替换）、flush_page(page_id)（写回脏页面）。
    - [DONE] 确保脏页面在关闭时或定期刷新。

- **事务管理器**（新增，用于支持 ACID 机制：原子性、一致性、隔离性、持久性）
  - 管理事务生命周期，确保操作符合 ACID 属性。
  - [DONE] 实现事务开始 (BEGIN TRANSACTION)：启动新事务上下文，记录日志。
  - [DONE] 实现事务提交 (COMMIT)：应用所有更改，刷新日志和数据到磁盘，确保持久性。
  - [DONE] 实现事务回滚 (ROLLBACK)：撤销更改，使用 undo 日志恢复到事务开始状态，确保原子性。
  - [DONE] 添加写前日志 (WAL - Write-Ahead Logging)：在实际写数据前记录操作日志，用于持久性和原子性（例如，使用单独日志文件记录 INSERT/DELETE 等操作）。
  - [PARTIALLY DONE] 实现简单隔离：使用页面级或行级锁（例如读写锁）防止并发冲突，确保隔离性（假设单线程简化，或添加基本锁机制）。
  - [PARTIALLY DONE] 确保一致性：在语义分析和执行中集成检查（如约束验证），并在事务结束时验证。
  - [DONE] 与执行引擎集成：在操作前检查事务状态，并在失败时触发回滚。
  - [DONE] 支持默认自动提交模式（无显式 BEGIN 时，每个语句作为一个事务）。
  - [PARTIALLY DONE] 处理崩溃恢复：启动时从 WAL 日志重放未提交事务，确保耐久性。

- **数据库核心层**
  - 将编译器输出与存储集成用于执行。
  - **系统目录管理器**
    - [DONE] 将 Catalog 实现为特殊表（例如 "system_catalog"），通过存储层存储。
    - [DONE] 存储元数据：表名、列名/类型、页面映射。
    - [DONE] 提供 API：get_table_schema(table_name)、register_table(table_name, schema)、操作时更新。
    - [DONE] 在启动时从存储加载 Catalog 以实现持久化。
  - **存储引擎**
    - [PARTIALLY DONE] 处理行-页面映射：将行序列化为字节（例如固定长度简化，处理类型如 INT=4 字节、VARCHAR=带长度前缀的变量）。
    - [PARTIALLY DONE] 将页面反序列化为行用于查询。
    - [PARTIALLY DONE] 管理页面中的空闲空间：跟踪每个页面的槽位，满时分配新页面。
    - [DONE] 与页面存储集成：所有 I/O 使用 get_page/write_page。
    - [PARTIALLY DONE] 支持删除：标记行删除（墓碑）或压缩页面。
    - [DONE] 与事务集成：在写操作时记录 WAL 日志。
  - **执行引擎**
    - [DONE] 执行编译器的计划：顺序解释运算符。
    - [DONE] 实现 CreateTable：在 Catalog 中注册，分配初始页面。
    - [DONE] 实现 Insert：将值序列化为行，通过存储引擎找到/分配页面，写入。
    - [DONE] 实现 SeqScan：迭代表的所有页面，反序列化为行。
    - [PARTIALLY DONE] 实现 Filter：在行上应用 WHERE 条件（支持基本谓词如 >、=）。
    - [DONE] 实现 Project：从过滤行中选择指定列。
    - [DONE] 实现 Delete：使用 Filter 扫描，标记/删除匹配行。
    - [DONE] 与事务集成：所有修改操作在事务上下文中执行，回滚时撤销。
    - [DONE] 将结果返回到 CLI（例如 SELECT 的行列表）。

### 附加考虑
- **持久化**：[PARTIALLY DONE] 所有数据（表、Catalog、WAL 日志）必须通过基于文件的存储在重启后存活。
- **错误处理**：[DONE] 从下层传播错误到 CLI，并提供详细消息，包括事务失败。
- **测试**：
  - [TODO] 为每个子模块编写单元测试（例如 lexer 的样本 SQL）。
  - [TODO] 集成测试：完整 SQL 流程如 CREATE -> INSERT -> SELECT -> DELETE。
  - [TODO] 持久化测试：重启并查询现有数据。
  - [TODO] 错误测试：无效语法、类型不匹配、不存在表。
  - [TODO] 事务测试：测试 COMMIT/ROLLBACK、崩溃恢复、多语句原子性。
- **可选扩展**（如果时间允许，根据文档）：
  - [DONE] 添加 UPDATE 支持。
  - [PARTIALLY DONE] 基本查询优化（例如谓词下推）。
  - [TODO] 支持 JOIN、ORDER BY、GROUP BY。