#include "sql_parser.h"
#include <stdexcept>
#include <sstream>

std::vector<std::string> tokenize(const std::string& sql) {
    std::vector<std::string> tokens;
    std::stringstream ss(sql);
    std::string token;
    while (ss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

ASTNode parse_sql(const std::string& sql) {
    auto tokens = tokenize(sql);
    if (tokens.empty()) throw std::runtime_error("Empty SQL");

    ASTNode ast;
    ast.type = tokens[0];
    
    if (ast.type == "SELECT") {
        if (tokens[1] != "*") ast.columns = {tokens[1]};
        ast.params["table"] = tokens[3];
        // Simplified WHERE (e.g., WHERE id=1)
        if (tokens.size() > 4 && tokens[4] == "WHERE") {
            ast.params["where"] = tokens[5];
        }
    } else if (ast.type == "INSERT") {
        ast.params["table"] = tokens[2];
        // Assume VALUES (val1, val2)
        ast.columns = {"val1", "val2"};  // Placeholder
    } else if (ast.type == "UPDATE") {
        ast.params["table"] = tokens[1];
        // Simplified SET col=val WHERE ...
    } else if (ast.type == "DELETE") {
        ast.params["table"] = tokens[2];
    } else if (ast.type == "CREATE") {
        ast.params["table"] = tokens[2];
        // Parse columns (id int, name text)
    } else {
        throw std::runtime_error("Unsupported SQL: " + ast.type);
    }
    return ast;
}