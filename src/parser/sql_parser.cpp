#include "sql_parser.h"
#include <stdexcept>
#include <sstream>
#include <cctype>
#include <algorithm>
#include <iostream>
#include <set>

const std::set<std::string> KEYWORDS = {
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
    "CREATE", "TABLE", "INDEX", "ON", "DROP", "BEGIN", "START", "COMMIT", "ROLLBACK", "VACUUM",
    "INT", "INTEGER", "TEXT", "VARCHAR", "AND", "LIKE", "NOT", "NULL", "AUTO_INCREMENT",
    "COMMENT", "DEFAULT", "PRIMARY", "KEY", "UNIQUE", "BIGINT", "TINYINT", "DATETIME",
    "CURRENT_TIMESTAMP", "ENGINE", "CHARSET", "COLLATE"
};

std::vector<Token> tokenize(const std::string& sql) {
    std::vector<Token> tokens;
    int line = 1;
    int col = 1;
    size_t i = 0;

    while (i < sql.length()) {
        char c = sql[i];

        if (std::isspace(c)) {
            if (c == '\r') {
                // Handle Windows-style line endings (\r\n)
                if (i + 1 < sql.length() && sql[i + 1] == '\n') {
                    i++; // Skip \r
                    i++; // Skip \n
                    line++;
                    col = 1;
                } else {
                    // Just \r (old Mac style)
                    line++;
                    col = 1;
                    i++;
                }
            } else if (c == '\n') {
                // Unix-style line ending
                line++;
                col = 1;
                i++;
            } else {
                // Other whitespace
                col++;
                i++;
            }
            continue;
        }

        if (c == '"' || c == '\'') {
            char quote = c;
            std::string text;
            text += c;
            i++;
            int start_col = col;
            col++;
            while (i < sql.length() && sql[i] != quote) {
                char current_char = sql[i];
                text += current_char;
                // Handle Chinese characters and other non-ASCII characters in strings
                if ((unsigned char)current_char > 127) {
                    // Multi-byte character, just add it and continue
                    i++;
                    col++;
                } else if (current_char == '\n') {
                    line++;
                    col = 1;
                    i++;
                } else {
                    i++;
                    col++;
                }
            }
            if (i < sql.length() && sql[i] == quote) {
                text += sql[i];
                i++;
                col++;
                tokens.push_back({TokenType::STRING_LITERAL, text, line, start_col});
            } else {
                throw std::runtime_error("Unclosed string literal at line " + std::to_string(line) + " col " + std::to_string(start_col));
            }
            continue;
        }

        if (std::isalpha(c)) {
            std::string text;
            int start_col = col;
            while (i < sql.length() && (std::isalnum(sql[i]) || sql[i] == '_')) {
                text += sql[i];
                i++;
                col++;
            }
            std::string upper_text = text;
            std::transform(upper_text.begin(), upper_text.end(), upper_text.begin(), ::toupper);
            if (KEYWORDS.count(upper_text)) {
                tokens.push_back({TokenType::KEYWORD, text, line, start_col});
            } else {
                tokens.push_back({TokenType::IDENTIFIER, text, line, start_col});
            }
            continue;
        }

        if (std::isdigit(c)) {
            std::string text;
            int start_col = col;
            while (i < sql.length() && std::isdigit(sql[i])) {
                text += sql[i];
                i++;
                col++;
            }
            tokens.push_back({TokenType::INTEGER_LITERAL, text, line, start_col});
            continue;
        }

        // Handle backticks for MySQL identifiers
        if (c == '`') {
            std::string text;
            i++; // skip opening backtick
            int start_col = col;
            col++;
            while (i < sql.length() && sql[i] != '`') {
                text += sql[i];
                i++;
                col++;
            }
            if (i < sql.length() && sql[i] == '`') {
                i++; // skip closing backtick
                col++;
                tokens.push_back({TokenType::IDENTIFIER, text, line, start_col});
            } else {
                throw std::runtime_error("Unclosed backtick identifier at line " + std::to_string(line) + " col " + std::to_string(start_col));
            }
            continue;
        }

        // Handle SQL comments (-- style)
        if (c == '-' && i + 1 < sql.length() && sql[i + 1] == '-') {
            // Skip the rest of the line, allowing Chinese characters and other non-ASCII characters
            while (i < sql.length() && sql[i] != '\n' && sql[i] != '\r') {
                i++;
            }
            // Don't increment i here, let the whitespace handler deal with line endings
            continue;
        }

        if (c == '(' || c == ')' || c == ',' || c == ';') {
            tokens.push_back({TokenType::DELIMITER, std::string(1, c), line, col});
            i++;
            col++;
            continue;
        }

        if (c == '=' || c == '<' || c == '>' || c == '!') {
            int start_col = col;
            std::string op(1, c);
            i++;
            col++;
            if (i < sql.length()) {
                if ((c == '!' || c == '<') && sql[i] == '=') {
                    op += '=';
                    i++;
                    col++;
                } else if (c == '<' && sql[i] == '>') {
                    op += '>';
                    i++;
                    col++;
                }
            }
            tokens.push_back({TokenType::OPERATOR, op, line, start_col});
            continue;
        }

        // Skip non-ASCII characters (like Chinese characters in comments)
        if ((unsigned char)c > 127) {
            i++;
            col++;
            continue;
        }

        throw std::runtime_error("Invalid character '" + std::string(1, c) + "' at line " + std::to_string(line) + " col " + std::to_string(col));
    }

    tokens.push_back({TokenType::END_OF_FILE, "", line, col});
    return tokens;
}


class Parser {
public:
    Parser(const std::vector<Token>& tokens) : tokens_(tokens), pos_(0) {}

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

        throw std::runtime_error("Unsupported SQL statement: " + peek().text + " at line " + std::to_string(peek().line) + " col " + std::to_string(peek().column));
    }

private:
    std::vector<Token> tokens_;
    size_t pos_;

    bool is_end() const { return pos_ >= tokens_.size() || tokens_[pos_].type == TokenType::END_OF_FILE; }
    
    Token peek(int offset = 0) const {
        if (pos_ + offset >= tokens_.size()) return tokens_.back(); // EOF token
        return tokens_[pos_ + offset];
    }

    std::string peek_upper(int offset = 0) const {
        std::string token_text = peek(offset).text;
        std::transform(token_text.begin(), token_text.end(), token_text.begin(), ::toupper);
        return token_text;
    }

    Token consume() {
        if (is_end()) throw std::runtime_error("Unexpected end of query.");
        return tokens_[pos_++];
    }

    void expect(const std::string& expected) {
        Token token = consume();
        std::string upper_token = token.text;
        std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), ::toupper);
        std::string upper_expected = expected;
        std::transform(upper_expected.begin(), upper_expected.end(), upper_expected.begin(), ::toupper);
        if (upper_token != upper_expected) {
            throw std::runtime_error("Expected '" + expected + "' but got '" + token.text + "' at line " + std::to_string(token.line) + " col " + std::to_string(token.column));
        }
    }

    Value parse_value() {
        Token token = consume();
        Value val;
        if (token.type == TokenType::STRING_LITERAL) {
            val.type = DataType::STRING;
            val.str_value = token.text.substr(1, token.text.length() - 2);
        } else if (token.type == TokenType::INTEGER_LITERAL) {
            try {
                val.type = DataType::INT;
                val.int_value = std::stoi(token.text);
            } catch (const std::invalid_argument& e) {
                throw std::runtime_error("Invalid integer literal: " + token.text + " at line " + std::to_string(token.line) + " col " + std::to_string(token.column));
            }
        } else {
            throw std::runtime_error("Unexpected token '" + token.text + "' when parsing value at line " + std::to_string(token.line) + " col " + std::to_string(token.column));
        }
        return val;
    }

    ASTNode parse_select() {
        ASTNode node;
        node.type = "SELECT";
        consume(); // consume SELECT

        // Parse columns
        if (peek().text == "*") {
            node.columns.push_back({ consume().text, DataType::STRING }); // Represent wildcard
        } else {
            while (peek_upper() != "FROM") {
                node.columns.push_back({ consume().text, DataType::STRING }); // Type is unknown at this stage
                if (peek().text == ",") consume();
                if (is_end()) throw std::runtime_error("Incomplete SELECT statement.");
            }
        }

        expect("FROM");
        node.table_name = consume().text;

        if (peek_upper() == "WHERE") {
            parse_where_clause(node);
        }

        return node;
    }

    void parse_where_clause(ASTNode& node) {
        expect("WHERE");
        while (!is_end() && peek_upper() != "LIMIT" && peek_upper() != "ORDER" && peek_upper() != "GROUP") {
            std::string column = consume().text;
            std::string op = consume().text;
            Value value = parse_value();
            
            if (op != "=" && op != "<" && op != ">" && op != "!=" && op != "<>" && op != "<=" && op != ">=" && op != "LIKE") {
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
        node.table_name = consume().text;

        expect("VALUES");
        expect("(");
        while (peek().text != ")") {
            node.values.push_back(parse_value());
            if (peek().text == ",") consume();
        }
        expect(")");
        return node;
    }
    
    ASTNode parse_update() {
        ASTNode node;
        node.type = "UPDATE";
        consume(); // consume UPDATE
        node.table_name = consume().text;
        expect("SET");

        while(peek_upper() != "WHERE" && !is_end()) {
            std::string col = consume().text;
            expect("=");
            node.set_clause[col] = parse_value();
            if(peek().text == ",") consume();
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
        node.table_name = consume().text;
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
            node.table_name = consume().text;
            expect("(");
            while (peek().text != ")") {
                // Check if this is a table-level constraint (PRIMARY KEY, UNIQUE KEY, KEY)
                std::string first_token = peek_upper();
                if (first_token == "PRIMARY" && peek_upper(1) == "KEY") {
                    // Handle table-level PRIMARY KEY
                    consume(); // PRIMARY
                    consume(); // KEY
                    expect("(");
                    consume(); // column name
                    expect(")");
                    if (peek().text == ",") consume();
                    continue;
                } else if (first_token == "UNIQUE" && peek_upper(1) == "KEY") {
                    // Handle table-level UNIQUE KEY
                    consume(); // UNIQUE
                    consume(); // KEY
                    consume(); // key name
                    expect("(");
                    consume(); // column name
                    expect(")");
                    if (peek().text == ",") consume();
                    continue;
                } else if (first_token == "KEY") {
                    // Handle table-level KEY (index)
                    consume(); // KEY
                    consume(); // key name
                    expect("(");
                    consume(); // column name
                    expect(")");
                    if (peek().text == ",") consume();
                    continue;
                }
                
                // Regular column definition
                std::string col_name = consume().text;
                std::string col_type_str = consume().text;
                std::transform(col_type_str.begin(), col_type_str.end(), col_type_str.begin(), ::toupper);

                // Handle column type with size specification like VARCHAR(50)
                if (col_type_str.rfind("VARCHAR", 0) == 0 || col_type_str.rfind("CHAR", 0) == 0) {
                    // Skip size specification if present
                    if (peek().text == "(") {
                        consume(); // (
                        consume(); // size number
                        expect(")");
                    }
                }

                DataType col_type;
                if (col_type_str == "INT" || col_type_str == "INTEGER" || col_type_str == "BIGINT" || col_type_str == "TINYINT") {
                    col_type = DataType::INT;
                } else if (col_type_str.rfind("VARCHAR", 0) == 0 || col_type_str == "TEXT" || col_type_str.rfind("CHAR", 0) == 0) {
                    col_type = DataType::STRING;
                } else if (col_type_str == "DATETIME") {
                    col_type = DataType::STRING; // Treat datetime as string for now
                } else {
                    throw std::runtime_error("Unsupported column type: " + col_type_str);
                }
                
                bool not_null = false;
                
                // Skip various MySQL-specific column attributes
                while (pos_ < tokens_.size() && peek().text != "," && peek().text != ")") {
                    std::string attr = peek_upper();
                    if (attr == "NOT" && peek_upper(1) == "NULL") {
                        consume(); // NOT
                        consume(); // NULL
                        not_null = true;
                    } else if (attr == "AUTO_INCREMENT") {
                        consume(); // Skip AUTO_INCREMENT
                    } else if (attr == "COMMENT") {
                        consume(); // COMMENT
                        if (peek().type == TokenType::STRING_LITERAL) {
                            consume(); // Skip comment string
                        }
                    } else if (attr == "DEFAULT") {
                        consume(); // DEFAULT
                        if (peek_upper() == "CURRENT_TIMESTAMP") {
                            consume(); // Skip CURRENT_TIMESTAMP
                        } else if (peek().type == TokenType::STRING_LITERAL || peek().type == TokenType::INTEGER_LITERAL) {
                            consume(); // Skip default value
                        }
                    } else if (attr == "ON" && peek_upper(1) == "UPDATE") {
                        consume(); // ON
                        consume(); // UPDATE
                        consume(); // CURRENT_TIMESTAMP
                    } else if (attr == "PRIMARY" && peek_upper(1) == "KEY") {
                        // Handle PRIMARY KEY inline - skip for now
                        consume(); // PRIMARY
                        consume(); // KEY
                        if (peek().text == "(") {
                            consume(); // (
                            consume(); // column name
                            expect(")");
                        }
                    } else if (attr == "UNIQUE" && peek_upper(1) == "KEY") {
                        // Handle UNIQUE KEY inline - skip for now
                        consume(); // UNIQUE
                        consume(); // KEY
                        consume(); // key name
                        if (peek().text == "(") {
                            consume(); // (
                            consume(); // column name
                            expect(")");
                        }
                    } else if (attr == "KEY") {
                        // Handle KEY (index) - skip for now
                        consume(); // KEY
                        consume(); // key name
                        if (peek().text == "(") {
                            consume(); // (
                            consume(); // column name
                            expect(")");
                        }
                    } else {
                        // Skip unknown attributes to avoid parsing errors
                        consume();
                    }
                }

                node.columns.push_back({col_name, col_type, not_null});

                if (peek().text == ",") consume();
            }
            expect(")");
            
            // Skip table options like ENGINE, CHARSET, etc.
            while (pos_ < tokens_.size() && peek().text != ";") {
                std::string option = peek_upper();
                if (option == "ENGINE" || option == "CHARSET" || option == "COLLATE") {
                    consume(); // option name
                    if (peek().text == "=") consume(); // =
                    consume(); // option value
                } else if (option == "COMMENT") {
                    consume(); // COMMENT
                    if (peek().text == "=") consume(); // =
                    if (peek().type == TokenType::STRING_LITERAL) {
                        consume(); // comment string
                    }
                } else {
                    break;
                }
            }
        } else if (next == "INDEX") {
            node.type = "CREATE_INDEX";
            consume(); // consume INDEX
            node.index_name = consume().text;
            expect("ON");
            node.table_name = consume().text;
            expect("(");
            node.index_column = consume().text;
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
            node.table_name = consume().text;
        } else if (next == "INDEX") {
            node.type = "DROP_INDEX";
            consume(); // consume INDEX
            node.index_name = consume().text;
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
        node.table_name = consume().text;
        return node;
    }
};

ASTNode parse_sql(const std::string& sql) {
    auto tokens = tokenize(sql);
    Parser parser(tokens);
    return parser.parse();
}

void print_ast(const ASTNode& node, int indent) {
    std::string indentation(indent * 2, ' ');
    std::cout << indentation << "type: " << node.type << std::endl;
    if (!node.table_name.empty()) {
        std::cout << indentation << "table_name: " << node.table_name << std::endl;
    }
    if (!node.index_name.empty()) {
        std::cout << indentation << "index_name: " << node.index_name << std::endl;
    }
    if (!node.index_column.empty()) {
        std::cout << indentation << "index_column: " << node.index_column << std::endl;
    }
    if (!node.columns.empty()) {
        std::cout << indentation << "columns:" << std::endl;
        for (const auto& col : node.columns) {
            std::cout << indentation << "  - name: " << col.name << ", type: " << (col.type == DataType::INT ? "INT" : "STRING") << std::endl;
        }
    }
    if (!node.values.empty()) {
        std::cout << indentation << "values:" << std::endl;
        for (const auto& val : node.values) {
            std::cout << indentation << "  - " << (val.type == DataType::INT ? std::to_string(val.int_value) : val.str_value) << std::endl;
        }
    }
    if (!node.set_clause.empty()) {
        std::cout << indentation << "set_clause:" << std::endl;
        for (const auto& pair : node.set_clause) {
            std::cout << indentation << "  - " << pair.first << " = " << (pair.second.type == DataType::INT ? std::to_string(pair.second.int_value) : pair.second.str_value) << std::endl;
        }
    }
    if (!node.where_conditions.empty()) {
        std::cout << indentation << "where_conditions:" << std::endl;
        for (const auto& cond : node.where_conditions) {
            std::cout << indentation << "  - " << cond.column << " " << cond.op << " " << (cond.value.type == DataType::INT ? std::to_string(cond.value.int_value) : cond.value.str_value) << std::endl;
        }
    }
}
