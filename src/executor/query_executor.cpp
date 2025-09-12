#include "query_executor.h"
#include <iostream>

void execute_query(const ASTNode& ast, StorageEngine& storage, TransactionManager& tx_manager, int tx_id, const std::map<int, int>& snapshot) {
    int cid = tx_manager.get_next_cid(tx_id);
    if (ast.type == "CREATE") {
        if (ast.params.count("index")) {
            storage.create_index(ast.params.at("table"), ast.params.at("column"));
        } else {
            storage.create_table(ast.params.at("table"), ast.columns);
        }
    } else if (ast.type == "INSERT") {
        Record rec{tx_id, 0, cid, ast.columns};
        storage.insert_record(ast.params.at("table"), rec, tx_id, cid);
    } else if (ast.type == "SELECT") {
        std::vector<Record> records;
        if (ast.params.count("scan_type") && ast.params.at("scan_type") == "index") {
            records = storage.index_scan(ast.params.at("table"), ast.params.at("index_col"), ast.params.at("index_val"), tx_id, cid, snapshot);
        } else {
            records = storage.scan_table(ast.params.at("table"), tx_id, cid, snapshot);
        }
        for (const auto& rec : records) {
            for (const auto& col : rec.columns) {
                std::cout << col << " ";
            }
            std::cout << std::endl;
        }
    } // Update/Delete similar, use cid
}