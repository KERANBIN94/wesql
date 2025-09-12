#include "sql_parser.h"
#include <stdexcept>
#include <sstream>
#include <cctype>
#include <algorithm>

// --- Tokenizer ---
std::vector<std::string> tokenize(const std::string& sql) {
    std::vector<std::string> tokens;
    std::string token;
    for (size_t i = 0; i < sql.length(); ++i) {
        char c = sql[i];
        if (c == '\'' || c == '"') {
            if (!token.empty()) {
                tokens.push_back(token);
                token.clear();
            }
            size_t end = sql.find(c, i + 1);
            if (end == std::string::npos) throw std::runtime_error("Unterminated string literal");
            tokens.push_back(sql.substr(i, end - i + 1));
            i = end;
        } else if (std::isspace(c)) {
            if (!token.empty()) {
                tokens.push_back(token);
                token.clear();
            }
        } else if (std::string("(),=*;<>").find(c) != std::string::npos) {
            if (!token.empty()) {
                tokens.push_back(token);
                token.clear();
            }
            tokens.push_back(std::string(1, c));
        } else {
            token += c;
        }
    }
    if (!token.empty()) {
        tokens.push_back(token);
    }
    return tokens;
}

// --- Parser ---

class Parser {
public:
    Parser(const std::vector<std::string>& tokens) : tokens_(tokens), pos_(0) {}

    ASTNode parse() {
        if (is_end()) throw std::runtime_error("Empty SQL query.");
        std::string type = peek();
        std::transform(type.begin(), type.end(), type.begin(), [](unsigned char c){ return std::toupper(c); });

        if (type == "SELECT") return parse_select();
        if (type == "INSERT") return parse_insert();
        if (type == "UPDATE") return parse_update();
        if (type == "DELETE") return parse_delete();
        if (type == "CREATE") return parse_create();

        throw std::runtime_error("Unsupported SQL statement: " + peek());
    }

private:
    std::vector<std::string> tokens_;
    size_t pos_;

    bool is_end() const { return pos_ >= tokens_.size(); }
    std::string peek() const { 
        if (is_end()) throw std::runtime_error("Unexpected end of query.");
        return tokens_[pos_]; 
    }
    std::string consume() { 
        if (is_end()) throw std::runtime_error("Unexpected end of query on consume.");
        return tokens_[pos_++]; 
    }

    void expect(const std::string& expected) {
        if (is_end()) throw std::runtime_error("Unexpected end of query. Expected: " + expected);
        std::string token = consume();
        std::string upper_token = token;
        std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), [](unsigned char c){ return std::toupper(c); });
        if (upper_token != expected) {
            throw std::runtime_error("Expected '" + expected + "' but got '" + token + "'");
        }
    }

    void parse_where_clause(ASTNode& node) {
        while (true) { 
            if (is_end() || peek() == ";") break;

            WhereCondition cond;
            cond.column = consume();
            cond.op = consume();
            cond.value = consume();
            node.where_conditions.push_back(cond);

            if (is_end() || peek() == ";") break;

            std::string next_token = peek();
            std::string upper_token = next_token;
            std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), [](unsigned char c){ return std::toupper(c); });

            if (upper_token == "AND") {
                consume();
            } else {
                break;
            }
        }
    }

    ASTNode parse_select() {
        ASTNode node;
        node.type = "SELECT";
        consume(); // consume SELECT

        while (!is_end()) {
            std::string token = peek();
            std::string upper_token = token;
            std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), [](unsigned char c){ return std::toupper(c); });
            if(upper_token == "FROM") break;

            node.columns.push_back(consume());
            if (!is_end() && peek() == ",") consume();
        }

        expect("FROM");
        node.table_name = consume();

        if (!is_end()) {
            std::string token = peek();
            std::string upper_token = token;
            std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), [](unsigned char c){ return std::toupper(c); });
            if(upper_token == "WHERE") {
                consume(); // consume WHERE
                parse_where_clause(node);
            }
        }
        return node;
    }

    ASTNode parse_insert() {
        ASTNode node;
        node.type = "INSERT";
        consume(); // consume INSERT
        expect("INTO");
        node.table_name = consume();

        expect("(");
        while (!is_end() && peek() != ")") {
            node.columns.push_back(consume());
            if (!is_end() && peek() == ",") consume();
        }
        expect(")");

        expect("VALUES");
        expect("(");
        while (!is_end() && peek() != ")") {
            node.values.push_back(consume());
            if (!is_end() && peek() == ",") consume();
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

        while (!is_end()) {
            std::string token = peek();
            std::string upper_token = token;
            std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), [](unsigned char c){ return std::toupper(c); });
            if(upper_token == "WHERE") break;

            std::string col = consume();
            expect("=");
            std::string val = consume();
            node.set_clause[col] = val;
            if (!is_end() && peek() == ",") consume();
        }

        if (!is_end()) {
            std::string token = peek();
            std::string upper_token = token;
            std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), [](unsigned char c){ return std::toupper(c); });
            if (upper_token == "WHERE") {
                consume();
                parse_where_clause(node);
            }
        }
        return node;
    }

    ASTNode parse_delete() {
        ASTNode node;
        node.type = "DELETE";
        consume(); // consume DELETE
        expect("FROM");
        node.table_name = consume();

        if (!is_end()) {
            std::string token = peek();
            std::string upper_token = token;
            std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), [](unsigned char c){ return std::toupper(c); });
            if (upper_token == "WHERE") {
                consume();
                parse_where_clause(node);
            }
        }
        return node;
    }

    ASTNode parse_create() {
        ASTNode node;
        node.type = "CREATE";
        consume(); // consume CREATE
        expect("TABLE");
        node.table_name = consume();

        expect("(");
        while (!is_end() && peek() != ")") {
            std::string col_name = consume();
            std::string col_type = consume();
            node.column_types.push_back({col_name, col_type});
            if (!is_end() && peek() == ",") consume();
        }
        expect(")");

        return node;
    }
};

ASTNode parse_sql(const std::string& sql) {
    auto tokens = tokenize(sql);
    Parser parser(tokens);
    return parser.parse();
}