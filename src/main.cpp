#include <iostream>
#include <string>
#include <vector>
#include "storage/storage_engine.h"
#include "parser/sql_parser.h"
#include "executor/query_executor.h"
#include "transaction/transaction_manager.h"
#include "optimizer/optimizer.h"
#include "buffer/buffer_cache.h"

int main() {
    BufferCache cache(100);
    StorageEngine storage(cache);
    cache.set_storage_engine(&storage);
    TransactionManager tx_manager;
    Optimizer optimizer;

    std::cout << "wesql DB. Enter SQL or 'exit' to quit." << std::endl;

    bool in_transaction = false;
    int current_tx_id = 0;

    while (true) {
        std::string sql_line;
        std::cout << "> ";
        std::getline(std::cin, sql_line);
        if (sql_line == "exit") break;
        if (sql_line.empty()) continue;

        try {
            auto ast = parse_sql(sql_line);

            if (ast.type == "BEGIN") {
                if (in_transaction) {
                    throw std::runtime_error("Already in a transaction block.");
                }
                current_tx_id = tx_manager.start_transaction();
                in_transaction = true;
                execute_query(ast, storage, tx_manager, 0, {}); // No tx context needed for BEGIN itself
            } else if (ast.type == "COMMIT") {
                if (!in_transaction) {
                    throw std::runtime_error("Not in a transaction block.");
                }
                tx_manager.commit(current_tx_id);
                in_transaction = false;
                current_tx_id = 0;
                execute_query(ast, storage, tx_manager, 0, {});
            } else if (ast.type == "ROLLBACK") {
                if (!in_transaction) {
                    throw std::runtime_error("Not in a transaction block.");
                }
                tx_manager.rollback(current_tx_id);
                in_transaction = false;
                current_tx_id = 0;
                execute_query(ast, storage, tx_manager, 0, {});
            } else {
                // Auto-commit mode or inside a transaction
                int tx_id_for_query = in_transaction ? current_tx_id : tx_manager.start_transaction();
                
                auto optimized_ast = optimizer.optimize(ast, storage);
                auto snapshot = tx_manager.get_snapshot(tx_id_for_query);
                execute_query(optimized_ast, storage, tx_manager, tx_id_for_query, snapshot);

                if (!in_transaction) {
                    tx_manager.commit(tx_id_for_query);
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            if (in_transaction) {
                std::cerr << "Rolling back current transaction." << std::endl;
                tx_manager.rollback(current_tx_id);
                in_transaction = false;
                current_tx_id = 0;
            }
        }
    }
    return 0;
}
