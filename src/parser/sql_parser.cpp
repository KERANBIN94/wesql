#include "sql_parser.h"
#include <stdexcept>
#include <sstream>
#include <cctype>
#include <algorithm>
#include <iostream>

// Simple tokenizer
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
            if (c != ' ' && c != ';') {
                tokens.push_back(std::string(1, c));
            }
        } else if (c == '=' || c == '<' || c == '>' || c == '!') {
             if (!current_token.empty()) {
                tokens.push_back(current_token);
                current_token = "";
            }
            if (i + 1 < sql.length()) {
                if ((c == '!' || c == '<') && sql[i+1] == '=') {
                    tokens.push_back(std::string(1, c) + "=");
                    i++; // Skip next char
                } else if (c == '<' && sql[i+1] == '>') {
                    tokens.push_back("<>");
                    i++; // Skip next char
                } else {
                    tokens.push_back(std::string(1, c));
                }
            } else {
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

class Parser {
public:
    Parser(const std::vector<std::string>& tokens) : tokens_(tokens), pos_(0) {}

    ASTNode parse() {
        if (is_end()) throw std::runtime_error("Empty SQL query.");
        std::string type = peek_upper();

        if (type == "SELECT") return parse_select();
        if (type == "INSERT") return parse_insert();
        if (type == "UPDATE") return parse_update();
        if (type == "DELETE") return parse_delete();
        if (type == "CREATE") return parse_create();
        if (type == "DROP") return parse_drop();
        if (type == "BEGIN" || type == "START") return parse_begin();
        if (type == "COMMIT") return parse_commit();
        if (type == "ROLLBACK") return parse_rollback();
        if (type == "VACUUM") return parse_vacuum();

        throw std::runtime_error("Unsupported SQL statement: " + peek());
    }

private:
    std::vector<std::string> tokens_;
    size_t pos_;

    bool is_end() const { return pos_ >= tokens_.size(); }
    std::string peek(int offset = 0) const {
        if (pos_ + offset >= tokens_.size()) return "";
        return tokens_[pos_ + offset];
    }
    std::string peek_upper(int offset = 0) const {
        std::string token = peek(offset);
        std::transform(token.begin(), token.end(), token.begin(), ::toupper);
        return token;
    }
    std::string consume() {
        if (is_end()) throw std::runtime_error("Unexpected end of query.");
        return tokens_[pos_++];
    }
    void expect(const std::string& expected) {
        std::string token = consume();
        std::string upper_token = token;
        std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), ::toupper);
        std::string upper_expected = expected;
        std::transform(upper_expected.begin(), upper_expected.end(), upper_expected.begin(), ::toupper);
        if (upper_token != upper_expected) {
            throw std::runtime_error("Expected '" + expected + "' but got '" + token + "'.");
        }
    }

    Value parse_value() {
        std::string token = consume();
        Value val;
        if (token.front() == '"') { // String literal
            val.type = DataType::STRING;
            val.str_value = token.substr(1, token.length() - 2);
        } else { // Integer literal
            try {
                val.type = DataType::INT;
                val.int_value = std::stoi(token);
            } catch (const std::invalid_argument& e) {
                throw std::runtime_error("Invalid integer literal: " + token);
            }
        }
        return val;
    }

    ASTNode parse_select() {
        ASTNode node;
        node.type = "SELECT";
        consume(); // consume SELECT

        // Parse columns
        if (peek() == "*") {
            node.columns.push_back({ "*", DataType::STRING }); // Represent wildcard
            consume();
        } else {
            while (peek_upper() != "FROM") {
                node.columns.push_back({ consume(), DataType::STRING }); // Type is unknown at this stage
                if (peek() == ",") consume();
                if (is_end()) throw std::runtime_error("Incomplete SELECT statement.");
            }
        }

        expect("FROM");
        node.table_name = consume();

        if (peek_upper() == "WHERE") {
            parse_where_clause(node);
        }

        return node;
    }

    void parse_where_clause(ASTNode& node) {
        expect("WHERE");
        while (!is_end() && peek_upper() != "LIMIT" && peek_upper() != "ORDER" && peek_upper() != "GROUP") {
            std::string column = consume();
            std::string op = consume();
            Value value = parse_value();
            
            if (op != "=" && op != "<" && op != ">" && op != "!=" && op != "<>" && op != "<=" && op != ">=" && peek_upper() != "LIKE") {
                 throw std::runtime_error("Unsupported operator in WHERE clause: " + op);
            }

            node.where_conditions.push_back({column, op, value});

            if (peek_upper() == "AND") {
                consume(); // consume AND
            } else {
                break; // No more conditions
            }
        }
    }

    ASTNode parse_insert() {
        ASTNode node;
        node.type = "INSERT";
        consume(); // consume INSERT
        expect("INTO");
        node.table_name = consume();

        // INSERT INTO users VALUES (...)
        expect("VALUES");
        expect("(");
        while (peek() != ")") {
            node.values.push_back(parse_value());
            if (peek() == ",") consume();
        }
        expect(")");
        return node;
    }
    
    ASTNode parse_update() {
        ASTNode node;
        node.type = "UPDATE";
        consume(); // consume UPDATE
        node.table_name = consume();
        expect("SET");

        while(peek_upper() != "WHERE" && !is_end()) {
            std::string col = consume();
            expect("=");
            node.set_clause[col] = parse_value();
            if(peek() == ",") consume();
        }

        if(peek_upper() == "WHERE") {
            parse_where_clause(node);
        }
        return node;
    }

    ASTNode parse_delete() {
        ASTNode node;
        node.type = "DELETE";
        consume(); // consume DELETE
        expect("FROM");
        node.table_name = consume();
        if(peek_upper() == "WHERE") {
            parse_where_clause(node);
        }
        return node;
    }

    ASTNode parse_create() {
        ASTNode node;
        consume(); // consume CREATE
        std::string next = peek_upper();
        if (next == "TABLE") {
            node.type = "CREATE_TABLE";
            consume(); // consume TABLE
            node.table_name = consume();
            expect("(");
            while (peek() != ")") {
                std::string col_name = consume();
                std::string col_type_str = consume();
                std::transform(col_type_str.begin(), col_type_str.end(), col_type_str.begin(), ::toupper);

                DataType col_type;
                if (col_type_str == "INT" || col_type_str == "INTEGER") {
                    col_type = DataType::INT;
                } else if (col_type_str.rfind("VARCHAR", 0) == 0 || col_type_str == "TEXT") {
                    col_type = DataType::STRING;
                } else {
                    throw std::runtime_error("Unsupported column type: " + col_type_str);
                }
                node.columns.push_back({col_name, col_type});

                if (peek() == ",") consume();
            }
            expect(")");
        } else if (next == "INDEX") {
            node.type = "CREATE_INDEX";
            consume(); // consume INDEX
            node.index_name = consume();
            expect("ON");
            node.table_name = consume();
            expect("(");
            node.index_column = consume();
            expect(")");
        } else {
            throw std::runtime_error("Unsupported CREATE statement. Must be CREATE TABLE or CREATE INDEX.");
        }
        return node;
    }

    ASTNode parse_drop() {
        consume(); // consume DROP
        std::string next = peek_upper();
        ASTNode node;
        if (next == "TABLE") {
            node.type = "DROP_TABLE";
            consume(); // consume TABLE
            node.table_name = consume();
        } else if (next == "INDEX") {
            node.type = "DROP_INDEX";
            consume(); // consume INDEX
            node.index_name = consume();
        } else {
            throw std::runtime_error("Unsupported DROP statement. Must be DROP TABLE or DROP INDEX.");
        }
        return node;
    }

    ASTNode parse_begin() {
        consume(); // consume BEGIN/START
        ASTNode node; 
        node.type = "BEGIN";
        return node;
    }

    ASTNode parse_commit() {
        consume(); // consume COMMIT
        ASTNode node; 
        node.type = "COMMIT";
        return node;
    }

    ASTNode parse_rollback() {
        consume(); // consumeROLLBACK
        ASTNode node; 
        node.type = "ROLLBACK";
        return node;
    }
    
    ASTNode parse_vacuum() {
        consume(); // consume VACUUM
        ASTNode node;
        node.type = "VACUUM";
        node.table_name = consume();
        return node;
    }
};

ASTNode parse_sql(const std::string& sql) {
    auto tokens = tokenize(sql);
    Parser parser(tokens);
    return parser.parse();
}