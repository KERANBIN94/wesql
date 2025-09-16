#include <iostream>
#include "src/storage/storage_engine.h"
#include "src/parser/sql_parser.h"
#include "src/executor/query_executor.h"
#include "src/transaction/transaction_manager.h"
#include "src/optimizer/optimizer.h"
#include "src/buffer/buffer_cache.h"

void print_result_set(const ResultSet& rs) {
    std::cout << "=== Query Results ===" << std::endl;
    for (const auto& col : rs.columns) {
        std::cout << col << "\t";
    }
    std::cout << std::endl;
    std::cout << "-------------------" << std::endl;
    for (const auto& row : rs.rows) {
        for (const auto& val : row) {
            std::cout << to_string(val) << "\t";
        }
        std::cout << std::endl;
    }
    std::cout << "(" << rs.rows.size() << " rows)" << std::endl;
}

int main() {
    try {
        BufferCache cache(100);
        StorageEngine storage(cache);
        cache.set_storage_engine(&storage);
        TransactionManager tx_manager(&storage);
        Optimizer optimizer(storage);

        std::cout << "Testing SELECT functionality..." << std::endl;

        // Start a transaction
        int tx_id = tx_manager.start_transaction();

        // Create table
        std::cout << "\n1. Creating table..." << std::endl;
        auto create_ast = parse_sql("CREATE TABLE Users (id INTEGER, name TEXT, age INTEGER);");
        auto create_plan = optimizer.optimize(create_ast);
        auto snapshot = tx_manager.get_snapshot(tx_id);
        execute_plan(create_plan, storage, tx_manager, tx_id, snapshot);
        std::cout << "Table created successfully." << std::endl;

        // Insert some data
        std::cout << "\n2. Inserting data..." << std::endl;
        auto insert1_ast = parse_sql("INSERT INTO Users VALUES (1, '张三', 25);");
        auto insert1_plan = optimizer.optimize(insert1_ast);
        execute_plan(insert1_plan, storage, tx_manager, tx_id, snapshot);

        auto insert2_ast = parse_sql("INSERT INTO Users VALUES (2, '李四', 30);");
        auto insert2_plan = optimizer.optimize(insert2_ast);
        execute_plan(insert2_plan, storage, tx_manager, tx_id, snapshot);

        auto insert3_ast = parse_sql("INSERT INTO Users VALUES (3, '王五', 22);");
        auto insert3_plan = optimizer.optimize(insert3_ast);
        execute_plan(insert3_plan, storage, tx_manager, tx_id, snapshot);
        std::cout << "Data inserted successfully." << std::endl;

        // Test SELECT *
        std::cout << "\n3. Testing SELECT * FROM Users;" << std::endl;
        auto select_all_ast = parse_sql("SELECT * FROM Users;");
        auto select_all_plan = optimizer.optimize(select_all_ast);
        snapshot = tx_manager.get_snapshot(tx_id);
        ResultSet rs1 = execute_plan(select_all_plan, storage, tx_manager, tx_id, snapshot);
        print_result_set(rs1);

        // Test SELECT specific columns
        std::cout << "\n4. Testing SELECT name, age FROM Users;" << std::endl;
        auto select_cols_ast = parse_sql("SELECT name, age FROM Users;");
        auto select_cols_plan = optimizer.optimize(select_cols_ast);
        snapshot = tx_manager.get_snapshot(tx_id);
        ResultSet rs2 = execute_plan(select_cols_plan, storage, tx_manager, tx_id, snapshot);
        print_result_set(rs2);

        // Commit transaction
        tx_manager.commit(tx_id);
        std::cout << "\nTransaction committed successfully." << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\nAll tests completed successfully!" << std::endl;
    return 0;
}