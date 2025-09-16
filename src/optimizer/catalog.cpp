#include "catalog.h"
#include "../storage/storage_engine.h"
#include <stdexcept>

bool Catalog::table_exists(const std::string& table_name) {
    if (!storage_engine_) {
        return false; // No storage engine, assume table doesn't exist
    }
    
    try {
        storage_engine_->get_table_metadata(table_name);
        return true;
    } catch (...) {
        return false;
    }
}

void Catalog::create_table(const TableSchema& schema) {
    // This method is called during semantic analysis, but the actual 
    // table creation is handled by StorageEngine in the executor
    // We'll just validate that the table doesn't already exist
    if (table_exists(schema.name)) {
        throw std::runtime_error("Table '" + schema.name + "' already exists.");
    }
    // Note: Don't actually create the table here, let StorageEngine do it
}

TableSchema Catalog::get_table_schema(const std::string& table_name) {
    if (!storage_engine_) {
        throw std::runtime_error("No storage engine configured");
    }
    
    try {
        const auto& columns = storage_engine_->get_table_metadata(table_name);
        TableSchema schema;
        schema.name = table_name;
        
        // Convert Column to ColumnDefinition
        for (const auto& col : columns) {
            ColumnDefinition col_def;
            col_def.name = col.name;
            col_def.type = col.type;
            col_def.not_null = col.not_null;
            schema.columns.push_back(col_def);
        }
        
        return schema;
    } catch (...) {
        throw std::runtime_error("Table '" + table_name + "' not found.");
    }
}
