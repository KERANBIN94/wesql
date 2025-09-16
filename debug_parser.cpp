#include "src/parser/sql_parser.h"
#include <iostream>

// 简单的tokenizer调试
std::vector<std::string> tokenize(const std::string& sql) {
    std::vector<std::string> tokens;
    std::string current_token;
    bool in_string = false;
    for (size_t i = 0; i < sql.length(); ++i) {
        char c = sql[i];
        if (c == '"') {
            in_string = !in_string;
            current_token += c;
            if (!in_string) {
                tokens.push_back(current_token);
                current_token = "";
            }
        } else if (in_string) {
            current_token += c;
        } else if (std::isspace(c) || c == '(' || c == ')' || c == ',' || c == ';') {
            if (!current_token.empty()) {
                tokens.push_back(current_token);
                current_token = "";
            }
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                tokens.push_back(std::string(1, c));
            }
        } else {
            current_token += c;
        }
    }
    if (!current_token.empty()) {
        tokens.push_back(current_token);
    }
    return tokens;
}

int main() {
    std::string sql = "CREATE TABLE temp_users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50));";
    std::cout << "SQL: " << sql << std::endl;
    std::cout << "Tokens:" << std::endl;
    
    auto tokens = tokenize(sql);
    for (size_t i = 0; i < tokens.size(); ++i) {
        std::cout << i << ": '" << tokens[i] << "'" << std::endl;
    }
    
    return 0;
}