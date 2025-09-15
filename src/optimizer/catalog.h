#ifndef CATALOG_H
#define CATALOG_H

#include <string>
#include <vector>
#include "../common/value.h"
#include "../parser/sql_parser.h"

struct TableSchema {
    std::string name;
    std::vector<ColumnDefinition> columns;
};

class Catalog {
public:
    bool table_exists(const std::string& table_name);
    void create_table(const TableSchema& schema);
    TableSchema get_table_schema(const std::string& table_name);
};

#endif