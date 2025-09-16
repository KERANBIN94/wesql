#include <iostream>
#include <string>
#include <vector>
#include "storage/storage_engine.h"
#include "parser/sql_parser.h"
#include "executor/query_executor.h"
#include "transaction/transaction_manager.h"
#include "optimizer/optimizer.h"
#include "buffer/buffer_cache.h"
#include "optimizer/plan_generator.h"

// #define DEBUG_AST
// #define DEBUG_PLAN

void print_result_set(const ResultSet& rs) {
    for (const auto& col : rs.columns) {
        std::cout << col << "\t";
    }
    std::cout << std::endl;
    for (const auto& row : rs.rows) {
        for (const auto& val : row) {
            std::cout << to_string(val) << "\t";
        }
        std::cout << std::endl;
    }
}

int main() {
    BufferCache cache(100);
    StorageEngine storage(cache);
    cache.set_storage_engine(&storage);
    TransactionManager tx_manager(&storage);
    Optimizer optimizer(storage);

    std::cout << "wesql DB. Enter SQL or 'exit' to quit." << std::endl;

    bool in_transaction = false;
    int current_tx_id = 0;

    std::string sql_query;
    while (true) {
        std::cout << (sql_query.empty() ? "> " : "-> ");
        std::string sql_line;
        std::getline(std::cin, sql_line);

        if (sql_line == "exit" && sql_query.empty()) {
            break;
        }

        // Allow canceling a multi-line query
        if (sql_line == "exit") {
            sql_query.clear();
            std::cout << "Query canceled." << std::endl;
            continue;
        }
        
        if (sql_query.empty() && sql_line.empty()) {
            continue;
        }

        sql_query += sql_line + "\n";

        size_t last_char_pos = sql_query.find_last_not_of(" \t\n\r");
        if (last_char_pos == std::string::npos || sql_query[last_char_pos] != ';') {
            continue; // Not a complete statement, get more input
        }

        try {
            auto ast = parse_sql(sql_query);

#ifdef DEBUG_AST
            print_ast(ast);
#endif

            if (ast.type == "BEGIN" || ast.type == "COMMIT" || ast.type == "ROLLBACK") {
                 // Handle transaction commands directly
                if (ast.type == "BEGIN") {
                    if (in_transaction) {
                        throw std::runtime_error("Already in a transaction block.");
                    }
                    current_tx_id = tx_manager.start_transaction();
                    in_transaction = true;
                } else if (ast.type == "COMMIT") {
                    if (!in_transaction) {
                        throw std::runtime_error("Not in a transaction block.");
                    }
                    tx_manager.commit(current_tx_id);
                    in_transaction = false;
                    current_tx_id = 0;
                } else { // ROLLBACK
                    if (!in_transaction) {
                        throw std::runtime_error("Not in a transaction block.");
                    }
                    tx_manager.rollback(current_tx_id);
                    in_transaction = false;
                    current_tx_id = 0;
                }
                // We can create a dummy plan for execution or handle in executor
                auto plan = std::make_shared<LogicalPlanNode>(LogicalOperatorType::CREATE_TABLE); // Dummy
                plan->table_name = ast.type; // Pass command type
                execute_plan(plan, storage, tx_manager, 0, {});

            } else {
                // Auto-commit mode or inside a transaction
                int tx_id_for_query = in_transaction ? current_tx_id : tx_manager.start_transaction();
                
                auto logical_plan = optimizer.optimize(ast);

#ifdef DEBUG_PLAN
                print_logical_plan(logical_plan);
#endif

                auto snapshot = tx_manager.get_snapshot(tx_id_for_query);
                ResultSet rs = execute_plan(logical_plan, storage, tx_manager, tx_id_for_query, snapshot);
                print_result_set(rs);

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
        // Reset for the next query
        sql_query.clear();
    }
    cache.flush_all();
    cache.print_stats();
    return 0;
}
