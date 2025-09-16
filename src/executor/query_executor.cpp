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
            
            int rows_inserted = 0;
            
            // Handle multi-row INSERT
            if (!plan->multi_values.empty()) {
                for (const auto& row_values : plan->multi_values) {
                    Record rec{tx_id, 0, cid, {}};
                    rec.columns = row_values;
                    storage.insert_record(plan->table_name, rec, tx_id, cid);
                    rows_inserted++;
                }
            } else {
                // Fallback to single-row INSERT for backward compatibility
                Record rec{tx_id, 0, cid, {}};
                rec.columns = plan->values;
                storage.insert_record(plan->table_name, rec, tx_id, cid);
                rows_inserted = 1;
            }
            
            std::cout << rows_inserted << " row(s) inserted." << std::endl;
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
            ResultSet rs;
            rs.columns = child_rs.columns;
            
            for (const auto& row : child_rs.rows) {
                bool row_matches = true;
                
                // Check all WHERE conditions
                for (const auto& condition : plan->conditions) {
                    bool condition_met = false;
                    
                    // Find the column index
                    int col_index = -1;
                    for (size_t i = 0; i < rs.columns.size(); ++i) {
                        if (rs.columns[i] == condition.column) {
                            col_index = i;
                            break;
                        }
                    }
                    
                    if (col_index == -1) {
                        throw std::runtime_error("Column '" + condition.column + "' not found in result set");
                    }
                    
                    // Apply the condition
                    const Value& col_value = row[col_index];
                    const Value& filter_value = condition.value;
                    
                    if (condition.op == "=") {
                        condition_met = (col_value.type == filter_value.type) &&
                                       ((col_value.type == DataType::INT && col_value.int_value == filter_value.int_value) ||
                                        (col_value.type == DataType::STRING && col_value.str_value == filter_value.str_value));
                    } else if (condition.op == "!=") {
                        condition_met = (col_value.type != filter_value.type) ||
                                       ((col_value.type == DataType::INT && col_value.int_value != filter_value.int_value) ||
                                        (col_value.type == DataType::STRING && col_value.str_value != filter_value.str_value));
                    } else if (condition.op == "<") {
                        if (col_value.type == DataType::INT && filter_value.type == DataType::INT) {
                            condition_met = col_value.int_value < filter_value.int_value;
                        } else if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                            condition_met = col_value.str_value < filter_value.str_value;
                        }
                    } else if (condition.op == ">") {
                        if (col_value.type == DataType::INT && filter_value.type == DataType::INT) {
                            condition_met = col_value.int_value > filter_value.int_value;
                        } else if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                            condition_met = col_value.str_value > filter_value.str_value;
                        }
                    } else if (condition.op == "<=") {
                        if (col_value.type == DataType::INT && filter_value.type == DataType::INT) {
                            condition_met = col_value.int_value <= filter_value.int_value;
                        } else if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                            condition_met = col_value.str_value <= filter_value.str_value;
                        }
                    } else if (condition.op == ">=") {
                        if (col_value.type == DataType::INT && filter_value.type == DataType::INT) {
                            condition_met = col_value.int_value >= filter_value.int_value;
                        } else if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                            condition_met = col_value.str_value >= filter_value.str_value;
                        }
                    } else if (condition.op == "LIKE") {
                        if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                            // Simple LIKE implementation (contains)
                            condition_met = col_value.str_value.find(filter_value.str_value) != std::string::npos;
                        }
                    }
                    
                    if (!condition_met) {
                        row_matches = false;
                        break; // AND logic: if any condition fails, the row doesn't match
                    }
                }
                
                if (row_matches) {
                    rs.rows.push_back(row);
                }
            }
            return rs;
        }
        case LogicalOperatorType::PROJECTION: {
            auto child_rs = execute_plan(plan->children[0], storage, tx_manager, tx_id, snapshot);
            ResultSet rs;
            
            // Handle wildcard projection (SELECT *)
            if (plan->projection_columns.size() == 1 && plan->projection_columns[0] == "*") {
                // Return all columns
                return child_rs;
            }
            
            // Handle specific column projection
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
        case LogicalOperatorType::UPDATE: {
            if (!tx_manager.lock_table(tx_id, plan->table_name, LockMode::EXCLUSIVE)) {
                throw std::runtime_error("Failed to acquire exclusive lock for UPDATE.");
            }
            int updated_rows = storage.update_records(plan->table_name, plan->conditions, plan->set_clause, tx_id, cid, snapshot, tx_manager);
            std::cout << updated_rows << " row(s) updated." << std::endl;
            return {};
        }
        case LogicalOperatorType::DELETE: {
            if (!tx_manager.lock_table(tx_id, plan->table_name, LockMode::EXCLUSIVE)) {
                throw std::runtime_error("Failed to acquire exclusive lock for DELETE.");
            }
            int deleted_rows = storage.delete_records(plan->table_name, plan->conditions, tx_id, cid, snapshot, tx_manager);
            std::cout << deleted_rows << " row(s) deleted." << std::endl;
            return {};
        }
        case LogicalOperatorType::CREATE_INDEX: {
            storage.create_index(plan->index_name, plan->table_name, plan->index_column);
            std::cout << "Index created." << std::endl;
            return {};
        }
        case LogicalOperatorType::DROP_TABLE: {
            storage.drop_table(plan->table_name);
            std::cout << "Table dropped." << std::endl;
            return {};
        }
        case LogicalOperatorType::DROP_INDEX: {
            storage.drop_index(plan->index_name);
            std::cout << "Index dropped." << std::endl;
            return {};
        }
        default: {
            throw std::runtime_error("Unsupported logical operator");
        }
    }
}