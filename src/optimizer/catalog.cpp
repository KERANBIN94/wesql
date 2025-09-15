#include "catalog.h"
#include <stdexcept>

// In-memory catalog for now
static std::vector<TableSchema> tables;

bool Catalog::table_exists(const std::string& table_name) {
    for (const auto& table : tables) {
        if (table.name == table_name) {
            return true;
        }
    }
    return false;
}

void Catalog::create_table(const TableSchema& schema) {
    if (table_exists(schema.name)) {
        throw std::runtime_error("Table '" + schema.name + "' already exists.");
    }
    tables.push_back(schema);
}

TableSchema Catalog::get_table_schema(const std::string& table_name) {
    for (const auto& table : tables) {
        if (table.name == table_name) {
            return table;
        }
    }
    throw std::runtime_error("Table '" + table_name + "' not found.");
}
