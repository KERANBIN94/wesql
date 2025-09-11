#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include <string>
#include <vector>
#include <map>

struct ASTNode {
    std::string type;  // SELECT, INSERT, etc.
    std::map<std::string, std::string> params;  // e.g., table_name, where_clause
    std::vector<std::string> columns;
    std::vector<ASTNode> children;
};

ASTNode parse_sql(const std::string& sql);

#endif