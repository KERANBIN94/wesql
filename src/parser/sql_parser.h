#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include <string>
#include <vector>
#include <map>
#include "../common/value.h"

struct WhereCondition {
    std::string column;
    std::string op;
    Value value;
};

struct ColumnDefinition {
    std::string name;
    DataType type;
};

struct ASTNode {
    std::string type;
    std::string table_name;
    std::string index_name;
    std::string index_column;
    std::vector<ColumnDefinition> columns;
    std::vector<Value> values;
    std::map<std::string, Value> set_clause;
    std::vector<WhereCondition> where_conditions;
    std::map<std::string, std::string> hints;
};

ASTNode parse_sql(const std::string& sql);

#endif