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
    "INT", "INTEGER", "TEXT", "VARCHAR", "AND", "LIKE"
};

std::vector<Token> tokenize(const std::string& sql) {
    std::vector<Token> tokens;
    int line = 1;
    int col = 1;
    size_t i = 0;

    while (i < sql.length()) {
        char c = sql[i];

        if (std::isspace(c)) {
            if (c == '\n') {
                line++;
                col = 1;
            } else {
                col++;
            }
            i++;
            continue;
        }

        if (c == '"' || c == '\'' || c == '`') {
            char quote_char = c;
            std::string text;
            i++; // Consume the opening quote
            int start_col = col;
            col++;
            while (i < sql.length() && sql[i] != quote_char) {
                text += sql[i];
                i++;
                col++;
            }
            if (i < sql.length() && sql[i] == quote_char) {
                i++; // Consume the closing quote
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

        if (c == '(' || c == ')' || c == ',' || c == ';') {
            tokens.push_back({TokenType::DELIMITER, std::string(1, c), line, col});
            i++;
            col++;
            continue;
        }

        if (c == '=' || c == '<' || c == '>' || c == '!' || c == '*') {
            int start_col = col;
            std::string op(1, c);
            i++;
            col++;
            if (i < sql.length() && c != '*') { // * is always single character
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
        if (pos_ + offset >= tokens_.size()) {
            if (tokens_.empty()) {
                return {TokenType::END_OF_FILE, "", 1, 1};
            }
            return tokens_.back(); // EOF token
        }
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
            val.str_value = token.text;
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
        
        // Parse multiple value sets: (1, 'a'), (2, 'b'), (3, 'c')
        do {
            expect("(");
            std::vector<Value> row_values;
            while (peek().text != ")") {
                row_values.push_back(parse_value());
                if (peek().text == ",") consume();
            }
            expect(")");
            node.multi_values.push_back(row_values);
            
            // Check if there's another row
            if (peek().text == ",") {
                consume(); // consume comma between value sets
            } else {
                break;
            }
        } while (!is_end() && peek().text != ";");
        
        // For backward compatibility, if only one row, also populate single values
        if (node.multi_values.size() == 1) {
            node.values = node.multi_values[0];
        }
        
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
                std::string col_name = consume().text;
                std::string col_type_str = consume().text;
                std::transform(col_type_str.begin(), col_type_str.end(), col_type_str.begin(), ::toupper);

                DataType col_type;
                if (col_type_str == "INT" || col_type_str == "INTEGER") {
                    col_type = DataType::INT;
                } else if (col_type_str == "VARCHAR") {
                    col_type = DataType::STRING;
                    // Handle VARCHAR(length) syntax
                    if (peek().text == "(") {
                        consume(); // consume (
                        consume(); // consume length number
                        expect(")"); // consume )
                    }
                } else if (col_type_str == "TEXT") {
                    col_type = DataType::STRING;
                } else {
                    throw std::runtime_error("Unsupported column type: " + col_type_str);
                }
                
                bool not_null = false;
                if (peek_upper() == "NOT" && peek_upper(1) == "NULL") {
                    consume(); // NOT
                    consume(); // NULL
                    not_null = true;
                }

                node.columns.push_back({col_name, col_type, not_null});

                if (peek().text == ",") consume();
            }
            expect(")");
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
    if (!node.multi_values.empty()) {
        std::cout << indentation << "multi_values:" << std::endl;
        for (size_t i = 0; i < node.multi_values.size(); ++i) {
            std::cout << indentation << "  row " << i << ":" << std::endl;
            for (const auto& val : node.multi_values[i]) {
                std::cout << indentation << "    - " << (val.type == DataType::INT ? std::to_string(val.int_value) : val.str_value) << std::endl;
            }
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
