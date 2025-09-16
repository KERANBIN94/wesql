#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include <string>
#include <vector>
#include <map>
#include "../common/value.h"

enum class TokenType {
    KEYWORD,
    IDENTIFIER,
    INTEGER_LITERAL,
    STRING_LITERAL,
    OPERATOR,
    DELIMITER,
    END_OF_FILE
};

struct Token {
    TokenType type;
    std::string text;
    int line;
    int column;
};

struct WhereCondition {
    std::string column;
    std::string op;
    Value value;
};

struct ColumnDefinition {
    std::string name;
    DataType type;
    bool not_null = false;
};

struct ASTNode {
    std::string type;
    std::string table_name;
    std::string index_name;
    std::string index_column;
    std::vector<ColumnDefinition> columns;
    std::vector<Value> values; // For single-row INSERT (backward compatibility)
    std::vector<std::vector<Value>> multi_values; // For multi-row INSERT
    std::map<std::string, Value> set_clause;
    std::vector<WhereCondition> where_conditions;
    std::map<std::string, std::string> hints;
};

ASTNode parse_sql(const std::string& sql);
void print_ast(const ASTNode& node, int indent = 0);

#endif