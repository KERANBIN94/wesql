#include <iostream>
#include <string>
#include <vector>
#include "storage/storage_engine.h"
#include "parser/sql_parser.h"
#include "executor/query_executor.h"
#include "transaction/transaction_manager.h"
#include "optimizer/optimizer.h"
#include "buffer/buffer_cache.h"
// Rest of the file unchanged
int main() {
    BufferCache cache(100);  // Cache with 100 pages capacity
    StorageEngine storage(cache);
    TransactionManager tx_manager;
    Optimizer optimizer;

    std::cout << "Expanded PostgreSQL-like DB. Enter SQL or 'exit' to quit." << std::endl;

    while (true) {
        std::string sql;
        std::cout << "> ";
        std::getline(std::cin, sql);
        if (sql == "exit") break;

        try {
            auto ast = parse_sql(sql);  // Parse to AST
            auto optimized_ast = optimizer.optimize(ast, storage);  // Optimize
            auto tx_id = tx_manager.start_transaction();
            auto snapshot = tx_manager.get_snapshot(tx_id);
            execute_query(optimized_ast, storage, tx_manager, tx_id, snapshot);
            tx_manager.commit(tx_id);
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            tx_manager.rollback(tx_manager.get_current_tx_id());
        }
    }
    return 0;
}