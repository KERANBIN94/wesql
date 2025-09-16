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
    if (rs.columns.empty()) {
        std::cout << "Query executed successfully." << std::endl;
        return;
    }
    
    for (const auto& col : rs.columns) {
        std::cout << col.name << "\t";
    }
    std::cout << std::endl;
}

int main() {
    BufferCache cache(100);
    StorageEngine storage(cache);
    cache.set_storage_engine(&storage);
    TransactionManager tx_manager(&storage);
    Optimizer optimizer;

    std::cout << "wesql DB. Enter SQL or 'exit' to quit." << std::endl;

    bool in_transaction = false;
    int current_tx_id = 0;

    std::string sql_query;
    while (true) {
        std::string sql_line;
        
        // Read lines until we get a complete statement (ending with semicolon)
        do {
            std::cout << (sql_statement.empty() ? "> " : "  ");
            std::getline(std::cin, sql_line);
            if (sql_line == "exit") {
                if (!sql_statement.empty()) {
                    std::cout << "Incomplete statement discarded." << std::endl;
                }
                goto exit_loop;
            }
            if (!sql_line.empty()) {
                if (!sql_statement.empty()) {
                    sql_statement += " ";
                }
                sql_statement += sql_line;
            }
        } while (sql_statement.empty() || sql_statement.back() != ';');
        
        // Remove the trailing semicolon for parsing
        if (!sql_statement.empty() && sql_statement.back() == ';') {
            sql_statement.pop_back();
        }

        try {
            auto ast = parse_sql(sql_statement);

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
                std::cout << "Transaction command executed." << std::endl;
            } else {
                int tx_id_for_query = in_transaction ? current_tx_id : tx_manager.start_transaction();
                
                auto logical_plan = optimizer.optimize(ast, storage);

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
    
exit_loop:
    cache.flush_all();
    cache.print_stats();
    return 0;
}
