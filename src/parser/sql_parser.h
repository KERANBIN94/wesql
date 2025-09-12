#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include <string>
#include <vector>
#include <map>

struct WhereCondition {
    std::string column;
    std::string op;
    std::string value;
};

struct ASTNode {
    std::string type;
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> column_types;
    std::map<std::string, std::string> set_clause;
    std::vector<WhereCondition> where_conditions;
};

ASTNode parse_sql(const std::string& sql);

#endif
