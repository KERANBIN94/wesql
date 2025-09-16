#ifndef CATALOG_H
#define CATALOG_H

#include <string>
#include <vector>
#include "../common/value.h"
#include "../parser/sql_parser.h"

// Forward declaration to avoid circular dependency
class StorageEngine;

struct TableSchema {
    std::string name;
    std::vector<ColumnDefinition> columns;
};

class Catalog {
public:
    Catalog(StorageEngine* storage = nullptr) : storage_engine_(storage) {}
    
    void set_storage_engine(StorageEngine* storage) { storage_engine_ = storage; }
    
    bool table_exists(const std::string& table_name);
    void create_table(const TableSchema& schema);
    TableSchema get_table_schema(const std::string& table_name);

private:
    StorageEngine* storage_engine_;
};

#endif