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
    } else if (ast.type == "INSERT") {
        if (!catalog.table_exists(ast.table_name)) {
            throw std::runtime_error("Table '" + ast.table_name + "' does not exist.");
        }
        TableSchema schema = catalog.get_table_schema(ast.table_name);
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