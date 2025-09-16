#include "semantic_analyzer.h"
#include <stdexcept>

void SemanticAnalyzer::analyze(ASTNode& ast, Catalog& catalog) {
    if (ast.type == "CREATE_TABLE") {
        TableSchema schema;
        schema.name = ast.table_name;
        schema.columns = ast.columns;
        catalog.create_table(schema);
    } else if (ast.type == "SELECT" || ast.type == "UPDATE" || ast.type == "DELETE") {
        if (!catalog.table_exists(ast.table_name)) {
            throw std::runtime_error("Table '" + ast.table_name + "' does not exist.");
        }
        TableSchema schema = catalog.get_table_schema(ast.table_name);
        
        // Validate WHERE conditions
        for (auto& cond : ast.where_conditions) {
            bool found = false;
            for (const auto& col : schema.columns) {
                if (col.name == cond.column) {
                    if (col.type != cond.value.type) {
                        throw std::runtime_error("Type mismatch for column '" + cond.column + "'.");
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw std::runtime_error("Column '" + cond.column + "' not found in table '" + ast.table_name + "'.");
            }
        }
        
        // Validate SET clause for UPDATE statements
        if (ast.type == "UPDATE") {
            for (const auto& set_pair : ast.set_clause) {
                bool found = false;
                for (const auto& col : schema.columns) {
                    if (col.name == set_pair.first) {
                        if (col.type != set_pair.second.type) {
                            throw std::runtime_error("Type mismatch for column '" + set_pair.first + "' in SET clause.");
                        }
                        if (col.not_null && set_pair.second.is_null()) {
                            throw std::runtime_error("NULL value in NOT NULL column '" + set_pair.first + "'.");
                        }
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw std::runtime_error("Column '" + set_pair.first + "' not found in table '" + ast.table_name + "'.");
                }
            }
        }
    } else if (ast.type == "INSERT") {
        if (!catalog.table_exists(ast.table_name)) {
            throw std::runtime_error("Table '" + ast.table_name + "' does not exist.");
        }
        TableSchema schema = catalog.get_table_schema(ast.table_name);
        
        // Handle multi-row INSERT validation
        if (!ast.multi_values.empty()) {
            for (size_t row_idx = 0; row_idx < ast.multi_values.size(); ++row_idx) {
                const auto& row_values = ast.multi_values[row_idx];
                if (schema.columns.size() != row_values.size()) {
                    throw std::runtime_error("Row " + std::to_string(row_idx + 1) + ": Column count doesn't match value count.");
                }
                for (size_t i = 0; i < schema.columns.size(); ++i) {
                    if (schema.columns[i].type != row_values[i].type) {
                        throw std::runtime_error("Row " + std::to_string(row_idx + 1) + ": Type mismatch for column '" + schema.columns[i].name + "'.");
                    }
                    if (schema.columns[i].not_null && row_values[i].is_null()) {
                        throw std::runtime_error("Row " + std::to_string(row_idx + 1) + ": NULL value in NOT NULL column '" + schema.columns[i].name + "'.");
                    }
                }
            }
        } else {
            // Fallback to single-row validation
            if (schema.columns.size() != ast.values.size()) {
                throw std::runtime_error("Column count doesn't match value count.");
            }
            for (size_t i = 0; i < schema.columns.size(); ++i) {
                if (schema.columns[i].type != ast.values[i].type) {
                    throw std::runtime_error("Type mismatch for column '" + schema.columns[i].name + "'.");
                }
                if (schema.columns[i].not_null && ast.values[i].is_null()) {
                    throw std::runtime_error("NULL value in NOT NULL column '" + schema.columns[i].name + "'.");
                }
            }
        }
    }
}