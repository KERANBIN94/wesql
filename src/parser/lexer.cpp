#include "lexer.h"
#include <cctype>
#include <sstream>
#include <algorithm>

// 静态成员初始化
std::unordered_map<std::string, TokenType> Lexer::keywords_;

// Token类方法实现
std::string Token::getTypeName() const {
    switch (type) {
        case TokenType::SELECT: return "SELECT";
        case TokenType::FROM: return "FROM";
        case TokenType::WHERE: return "WHERE";
        case TokenType::INSERT: return "INSERT";
        case TokenType::INTO: return "INTO";
        case TokenType::VALUES: return "VALUES";
        case TokenType::UPDATE: return "UPDATE";
        case TokenType::SET: return "SET";
        case TokenType::DELETE: return "DELETE";
        case TokenType::CREATE: return "CREATE";
        case TokenType::TABLE: return "TABLE";
        case TokenType::DROP: return "DROP";
        case TokenType::ALTER: return "ALTER";
        case TokenType::INDEX: return "INDEX";
        case TokenType::PRIMARY: return "PRIMARY";
        case TokenType::KEY: return "KEY";
        case TokenType::FOREIGN: return "FOREIGN";
        case TokenType::REFERENCES: return "REFERENCES";
        case TokenType::AND: return "AND";
        case TokenType::OR: return "OR";
        case TokenType::NOT: return "NOT";
        case TokenType::NULL_TOKEN: return "NULL";
        case TokenType::TRUE_TOKEN: return "TRUE";
        case TokenType::FALSE_TOKEN: return "FALSE";
        case TokenType::DISTINCT: return "DISTINCT";
        case TokenType::ORDER: return "ORDER";
        case TokenType::BY: return "BY";
        case TokenType::GROUP: return "GROUP";
        case TokenType::HAVING: return "HAVING";
        case TokenType::LIMIT: return "LIMIT";
        case TokenType::OFFSET: return "OFFSET";
        case TokenType::JOIN: return "JOIN";
        case TokenType::INNER: return "INNER";
        case TokenType::LEFT: return "LEFT";
        case TokenType::RIGHT: return "RIGHT";
        case TokenType::OUTER: return "OUTER";
        case TokenType::ON: return "ON";
        case TokenType::UNION: return "UNION";
        case TokenType::INTERSECT: return "INTERSECT";
        case TokenType::EXCEPT: return "EXCEPT";
        case TokenType::AS: return "AS";
        case TokenType::CASE: return "CASE";
        case TokenType::WHEN: return "WHEN";
        case TokenType::THEN: return "THEN";
        case TokenType::ELSE: return "ELSE";
        case TokenType::END: return "END";
        case TokenType::IF: return "IF";
        case TokenType::EXISTS: return "EXISTS";
        case TokenType::BEGIN: return "BEGIN";
        case TokenType::COMMIT: return "COMMIT";
        case TokenType::ROLLBACK: return "ROLLBACK";
        case TokenType::TRANSACTION: return "TRANSACTION";
        case TokenType::VACUUM: return "VACUUM";
        case TokenType::ANALYZE: return "ANALYZE";
        case TokenType::INT: return "INT";
        case TokenType::VARCHAR: return "VARCHAR";
        case TokenType::CHAR: return "CHAR";
        case TokenType::TEXT: return "TEXT";
        case TokenType::REAL: return "REAL";
        case TokenType::BOOLEAN: return "BOOLEAN";
        case TokenType::DATE: return "DATE";
        case TokenType::TIME: return "TIME";
        case TokenType::TIMESTAMP: return "TIMESTAMP";
        case TokenType::IDENTIFIER: return "IDENTIFIER";
        case TokenType::INTEGER_LITERAL: return "INTEGER_LITERAL";
        case TokenType::REAL_LITERAL: return "REAL_LITERAL";
        case TokenType::STRING_LITERAL: return "STRING_LITERAL";
        case TokenType::PLUS: return "PLUS";
        case TokenType::MINUS: return "MINUS";
        case TokenType::MULTIPLY: return "MULTIPLY";
        case TokenType::DIVIDE: return "DIVIDE";
        case TokenType::MODULO: return "MODULO";
        case TokenType::EQUAL: return "EQUAL";
        case TokenType::NOT_EQUAL: return "NOT_EQUAL";
        case TokenType::LESS_THAN: return "LESS_THAN";
        case TokenType::LESS_EQUAL: return "LESS_EQUAL";
        case TokenType::GREATER_THAN: return "GREATER_THAN";
        case TokenType::GREATER_EQUAL: return "GREATER_EQUAL";
        case TokenType::SEMICOLON: return "SEMICOLON";
        case TokenType::COMMA: return "COMMA";
        case TokenType::LEFT_PAREN: return "LEFT_PAREN";
        case TokenType::RIGHT_PAREN: return "RIGHT_PAREN";
        case TokenType::DOT: return "DOT";
        case TokenType::END_OF_FILE: return "END_OF_FILE";
        case TokenType::UNKNOWN: return "UNKNOWN";
        case TokenType::ERROR_TOKEN: return "ERROR_TOKEN";
        default: return "UNKNOWN";
    }
}

std::string Token::toString() const {
    std::ostringstream oss;
    oss << "[" << static_cast<int>(type) << "," << value << "," << line << "," << column << "]";
    return oss.str();
}

// Lexer类方法实现
Lexer::Lexer(const std::string& input) 
    : input_(input), current_(0), current_line_(1), current_column_(1) {
    if (keywords_.empty()) {
        initializeKeywords();
    }
}

void Lexer::initializeKeywords() {
    keywords_["SELECT"] = TokenType::SELECT;
    keywords_["FROM"] = TokenType::FROM;
    keywords_["WHERE"] = TokenType::WHERE;
    keywords_["INSERT"] = TokenType::INSERT;
    keywords_["INTO"] = TokenType::INTO;
    keywords_["VALUES"] = TokenType::VALUES;
    keywords_["UPDATE"] = TokenType::UPDATE;
    keywords_["SET"] = TokenType::SET;
    keywords_["DELETE"] = TokenType::DELETE;
    keywords_["CREATE"] = TokenType::CREATE;
    keywords_["TABLE"] = TokenType::TABLE;
    keywords_["DROP"] = TokenType::DROP;
    keywords_["ALTER"] = TokenType::ALTER;
    keywords_["INDEX"] = TokenType::INDEX;
    keywords_["PRIMARY"] = TokenType::PRIMARY;
    keywords_["KEY"] = TokenType::KEY;
    keywords_["FOREIGN"] = TokenType::FOREIGN;
    keywords_["REFERENCES"] = TokenType::REFERENCES;
    keywords_["AND"] = TokenType::AND;
    keywords_["OR"] = TokenType::OR;
    keywords_["NOT"] = TokenType::NOT;
    keywords_["NULL"] = TokenType::NULL_TOKEN;
    keywords_["TRUE"] = TokenType::TRUE_TOKEN;
    keywords_["FALSE"] = TokenType::FALSE_TOKEN;
    keywords_["DISTINCT"] = TokenType::DISTINCT;
    keywords_["ORDER"] = TokenType::ORDER;
    keywords_["BY"] = TokenType::BY;
    keywords_["GROUP"] = TokenType::GROUP;
    keywords_["HAVING"] = TokenType::HAVING;
    keywords_["LIMIT"] = TokenType::LIMIT;
    keywords_["OFFSET"] = TokenType::OFFSET;
    keywords_["JOIN"] = TokenType::JOIN;
    keywords_["INNER"] = TokenType::INNER;
    keywords_["LEFT"] = TokenType::LEFT;
    keywords_["RIGHT"] = TokenType::RIGHT;
    keywords_["OUTER"] = TokenType::OUTER;
    keywords_["ON"] = TokenType::ON;
    keywords_["UNION"] = TokenType::UNION;
    keywords_["INTERSECT"] = TokenType::INTERSECT;
    keywords_["EXCEPT"] = TokenType::EXCEPT;
    keywords_["AS"] = TokenType::AS;
    keywords_["CASE"] = TokenType::CASE;
    keywords_["WHEN"] = TokenType::WHEN;
    keywords_["THEN"] = TokenType::THEN;
    keywords_["ELSE"] = TokenType::ELSE;
    keywords_["END"] = TokenType::END;
    keywords_["IF"] = TokenType::IF;
    keywords_["EXISTS"] = TokenType::EXISTS;
    keywords_["BEGIN"] = TokenType::BEGIN;
    keywords_["COMMIT"] = TokenType::COMMIT;
    keywords_["ROLLBACK"] = TokenType::ROLLBACK;
    keywords_["TRANSACTION"] = TokenType::TRANSACTION;
    keywords_["VACUUM"] = TokenType::VACUUM;
    keywords_["ANALYZE"] = TokenType::ANALYZE;
    keywords_["INT"] = TokenType::INT;
    keywords_["VARCHAR"] = TokenType::VARCHAR;
    keywords_["CHAR"] = TokenType::CHAR;
    keywords_["TEXT"] = TokenType::TEXT;
    keywords_["REAL"] = TokenType::REAL;
    keywords_["BOOLEAN"] = TokenType::BOOLEAN;
    keywords_["DATE"] = TokenType::DATE;
    keywords_["TIME"] = TokenType::TIME;
    keywords_["TIMESTAMP"] = TokenType::TIMESTAMP;
}

Token Lexer::nextToken() {
    skipWhitespace();
    
    if (isAtEnd()) {
        return makeToken(TokenType::END_OF_FILE, "");
    }
    
    char c = advance();
    
    // 字符串字面量
    if (c == '\'' || c == '"') {
        current_--; // 回退，让scanString处理
        return scanString();
    }
    
    // 数字
    if (isDigit(c)) {
        current_--; // 回退，让scanNumber处理
        return scanNumber();
    }
    
    // 标识符和关键字
    if (isAlpha(c)) {
        current_--; // 回退，让scanIdentifier处理
        return scanIdentifier();
    }
    
    // 运算符和分隔符
    switch (c) {
        case '+': return makeToken(TokenType::PLUS, "+");
        case '-': return makeToken(TokenType::MINUS, "-");
        case '*': return makeToken(TokenType::MULTIPLY, "*");
        case '/': return makeToken(TokenType::DIVIDE, "/");
        case '%': return makeToken(TokenType::MODULO, "%");
        case '(': return makeToken(TokenType::LEFT_PAREN, "(");
        case ')': return makeToken(TokenType::RIGHT_PAREN, ")");
        case ',': return makeToken(TokenType::COMMA, ",");
        case ';': return makeToken(TokenType::SEMICOLON, ";");
        case '.': return makeToken(TokenType::DOT, ".");
        case '=': return makeToken(TokenType::EQUAL, "=");
        case '!':
            if (match('=')) {
                return makeToken(TokenType::NOT_EQUAL, "!=");
            }
            return makeErrorToken("Unexpected character '!'");
        case '<':
            if (match('=')) {
                return makeToken(TokenType::LESS_EQUAL, "<=");
            } else if (match('>')) {
                return makeToken(TokenType::NOT_EQUAL, "<>");
            }
            return makeToken(TokenType::LESS_THAN, "<");
        case '>':
            if (match('=')) {
                return makeToken(TokenType::GREATER_EQUAL, ">=");
            }
            return makeToken(TokenType::GREATER_THAN, ">");
        default:
            return makeErrorToken("Unexpected character '" + std::string(1, c) + "'");
    }
}

std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;
    
    while (!isAtEnd()) {
        Token token = nextToken();
        tokens.push_back(token);
        
        if (token.type == TokenType::END_OF_FILE || token.type == TokenType::ERROR_TOKEN) {
            break;
        }
    }
    
    return tokens;
}

bool Lexer::isAtEnd() const {
    return current_ >= input_.length();
}

char Lexer::advance() {
    if (isAtEnd()) return '\0';
    
    char c = input_[current_++];
    if (c == '\n') {
        current_line_++;
        current_column_ = 1;
    } else {
        current_column_++;
    }
    return c;
}

char Lexer::peek() const {
    if (isAtEnd()) return '\0';
    return input_[current_];
}

char Lexer::peekNext() const {
    if (current_ + 1 >= input_.length()) return '\0';
    return input_[current_ + 1];
}

bool Lexer::match(char expected) {
    if (isAtEnd()) return false;
    if (input_[current_] != expected) return false;
    
    current_++;
    current_column_++;
    return true;
}

void Lexer::skipWhitespace() {
    while (!isAtEnd()) {
        char c = peek();
        if (c == ' ' || c == '\r' || c == '\t' || c == '\n') {
            advance();
        } else {
            break;
        }
    }
}

Token Lexer::makeToken(TokenType type, const std::string& value) {
    return Token(type, value, current_line_, current_column_ - value.length());
}

Token Lexer::makeErrorToken(const std::string& message) {
    addError(message);
    return Token(TokenType::ERROR_TOKEN, message, current_line_, current_column_);
}

Token Lexer::scanString() {
    int start_line = current_line_;
    int start_column = current_column_;
    
    char quote = advance(); // 消费开始的引号
    std::string value;
    
    while (!isAtEnd() && peek() != quote) {
        if (peek() == '\n') {
            addError("Unterminated string literal");
            return makeErrorToken("Unterminated string literal");
        }
        
        char c = advance();
        if (c == '\\' && !isAtEnd()) {
            // 处理转义字符
            char escaped = advance();
            switch (escaped) {
                case 'n': value += '\n'; break;
                case 't': value += '\t'; break;
                case 'r': value += '\r'; break;
                case '\\': value += '\\'; break;
                case '\'': value += '\''; break;
                case '"': value += '"'; break;
                default:
                    value += escaped;
                    break;
            }
        } else {
            value += c;
        }
    }
    
    if (isAtEnd()) {
        addError("Unterminated string literal");
        return makeErrorToken("Unterminated string literal");
    }
    
    advance(); // 消费结束的引号
    return Token(TokenType::STRING_LITERAL, value, start_line, start_column);
}

Token Lexer::scanNumber() {
    int start_line = current_line_;
    int start_column = current_column_;
    
    std::string value;
    bool is_real = false;
    
    // 扫描整数部分
    while (!isAtEnd() && isDigit(peek())) {
        value += advance();
    }
    
    // 检查小数点
    if (!isAtEnd() && peek() == '.' && isDigit(peekNext())) {
        is_real = true;
        value += advance(); // 消费小数点
        
        // 扫描小数部分
        while (!isAtEnd() && isDigit(peek())) {
            value += advance();
        }
    }
    
    TokenType type = is_real ? TokenType::REAL_LITERAL : TokenType::INTEGER_LITERAL;
    return Token(type, value, start_line, start_column);
}

Token Lexer::scanIdentifier() {
    int start_line = current_line_;
    int start_column = current_column_;
    
    std::string value;
    
    while (!isAtEnd() && isAlphaNumeric(peek())) {
        value += advance();
    }
    
    // 转换为大写以检查关键字
    std::string upper_value = value;
    std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
    
    auto it = keywords_.find(upper_value);
    TokenType type = (it != keywords_.end()) ? it->second : TokenType::IDENTIFIER;
    
    return Token(type, value, start_line, start_column);
}

Token Lexer::scanOperator() {
    // 这个方法在当前实现中不需要，因为运算符在nextToken中直接处理
    return makeToken(TokenType::UNKNOWN, "");
}

bool Lexer::isAlpha(char c) const {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

bool Lexer::isDigit(char c) const {
    return c >= '0' && c <= '9';
}

bool Lexer::isAlphaNumeric(char c) const {
    return isAlpha(c) || isDigit(c);
}

void Lexer::addError(const std::string& message) {
    std::ostringstream oss;
    oss << "Line " << current_line_ << ", Column " << current_column_ << ": " << message;
    errors_.push_back(oss.str());
}