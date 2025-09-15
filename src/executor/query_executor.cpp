#include "query_executor.h"
#include <iostream>
#include <vector>
#include "../common/value.h"

// Helper to print a Value
void print_value(const Value& val) {
    std::cout << to_string(val);
}

void execute_query(const ASTNode& ast, StorageEngine& storage, TransactionManager& tx_manager, int tx_id, const std::map<int, int>& snapshot) {
    int cid = 0;
    if (tx_id != 0) {
        cid = tx_manager.get_next_cid(tx_id);
    }

    if (ast.type == "BEGIN") {
        std::cout << "BEGIN" << std::endl;
    } else if (ast.type == "COMMIT") {
        std::cout << "COMMIT" << std::endl;
    } else if (ast.type == "ROLLBACK") {
        std::cout << "ROLLBACK" << std::endl;
    } else if (ast.type == "CREATE_TABLE") {
        storage.create_table(ast.table_name, ast.columns);
        std::cout << "Table created." << std::endl;
    } else if (ast.type == "CREATE_INDEX") {
        storage.create_index(ast.table_name, ast.index_column);
        std::cout << "Index created." << std::endl;
    } else if (ast.type == "DROP_TABLE") {
        storage.drop_table(ast.table_name);
        std::cout << "Table dropped." << std::endl;
    } else if (ast.type == "DROP_INDEX") {
        storage.drop_index(ast.index_name);
        std::cout << "Index dropped." << std::endl;
    } else if (ast.type == "INSERT") {
        // Type validation could be added here by checking against table metadata
        Record rec{tx_id, 0, cid, {}};
        rec.columns = ast.values;
        storage.insert_record(ast.table_name, rec, tx_id, cid);
        std::cout << "1 row inserted." << std::endl;
    } else if (ast.type == "SELECT") {
        auto records = storage.scan_table(ast.table_name, tx_id, cid, snapshot, tx_manager);
        const auto& table_cols = storage.get_table_metadata(ast.table_name);

        // Print header
        for(const auto& col : table_cols) {
            std::cout << col.name << "\t";
        }
        std::cout << std::endl;

        // Print records
        for (const auto& rec : records) {
            for (const auto& val : rec.columns) {
                print_value(val);
                std::cout << "\t";
            }
            std::cout << std::endl;
        }
    } else if (ast.type == "UPDATE") {
        int updated_count = storage.update_records(ast.table_name, ast.where_conditions, ast.set_clause, tx_id, cid, snapshot, tx_manager);
        std::cout << updated_count << " rows updated." << std::endl;
    } else if (ast.type == "DELETE") {
        int deleted_count = storage.delete_records(ast.table_name, ast.where_conditions, tx_id, cid, snapshot, tx_manager);
        std::cout << deleted_count << " rows deleted." << std::endl;
    } else if (ast.type == "VACUUM") {
        storage.vacuum_table(ast.table_name, tx_manager);
        std::cout << "Vacuum finished." << std::endl;
    }
}
