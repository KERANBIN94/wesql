#include "query_executor.h"
#include <iostream>
#include <vector>
#include "../common/value.h"
#include <algorithm>

// Helper to print a Value
void print_value(const Value& val) {
    std::cout << to_string(val);
}

ResultSet execute_plan(std::shared_ptr<LogicalPlanNode> plan, StorageEngine& storage, TransactionManager& tx_manager, int tx_id, const std::map<int, int>& snapshot) {
    if (!plan) return {};

    int cid = 0;
    if (tx_id != 0) {
        cid = tx_manager.get_next_cid(tx_id);
    }

    switch (plan->type) {
        case LogicalOperatorType::CREATE_TABLE: {
            if (plan->table_name == "BEGIN" || plan->table_name == "COMMIT" || plan->table_name == "ROLLBACK") {
                 std::cout << plan->table_name << std::endl;
            } else {
                if (!tx_manager.lock_table(tx_id, plan->table_name, LockMode::EXCLUSIVE)) {
                    throw std::runtime_error("Failed to acquire exclusive lock for CREATE TABLE.");
                }
                storage.create_table(plan->table_name, plan->columns, tx_id, cid);
                std::cout << "Table created." << std::endl;
            }
            return {};
        }
        case LogicalOperatorType::INSERT: {
            if (!tx_manager.lock_table(tx_id, plan->table_name, LockMode::EXCLUSIVE)) {
                throw std::runtime_error("Failed to acquire exclusive lock for INSERT.");
            }
            Record rec{tx_id, 0, cid, {}};
            rec.columns = plan->values;
            storage.insert_record(plan->table_name, rec, tx_id, cid);
            std::cout << "1 row inserted." << std::endl;
            return {};
        }
        case LogicalOperatorType::SEQ_SCAN: {
            if (!tx_manager.lock_table(tx_id, plan->table_name, LockMode::SHARED)) {
                throw std::runtime_error("Failed to acquire shared lock for SELECT.");
            }
            auto records = storage.scan_table(plan->table_name, tx_id, cid, snapshot, tx_manager);
            const auto& table_cols = storage.get_table_metadata(plan->table_name);
            
            ResultSet rs;
            for(const auto& col : table_cols) {
                rs.columns.push_back(col.name);
            }
            for (const auto& rec : records) {
                rs.rows.push_back(rec.columns);
            }
            return rs;
        }
        case LogicalOperatorType::FILTER: {
            auto child_rs = execute_plan(plan->children[0], storage, tx_manager, tx_id, snapshot);
            // This is a simplified filter implementation
            // It assumes the filter is always on the first column
            ResultSet rs;
            rs.columns = child_rs.columns;
            for (const auto& row : child_rs.rows) {
                if (row[0].int_value > 0) { // Dummy condition
                    rs.rows.push_back(row);
                }
            }
            return rs;
        }
        case LogicalOperatorType::PROJECTION: {
            auto child_rs = execute_plan(plan->children[0], storage, tx_manager, tx_id, snapshot);
            ResultSet rs;
            std::vector<int> proj_indices;
            for (const auto& proj_col : plan->projection_columns) {
                for (size_t i = 0; i < child_rs.columns.size(); ++i) {
                    if (child_rs.columns[i] == proj_col) {
                        rs.columns.push_back(proj_col);
                        proj_indices.push_back(i);
                        break;
                    }
                }
            }
            for (const auto& row : child_rs.rows) {
                std::vector<Value> new_row;
                for (int idx : proj_indices) {
                    new_row.push_back(row[idx]);
                }
                rs.rows.push_back(new_row);
            }
            return rs;
        }
        default: {
            throw std::runtime_error("Unsupported logical operator");
        }
    }
}