#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

// Token类型枚举
enum class TokenType {
    // 关键字
    SELECT = 1, FROM, WHERE, INSERT, INTO, VALUES, UPDATE, SET, DELETE, 
    CREATE, TABLE, DROP, ALTER, INDEX, PRIMARY, KEY, FOREIGN, REFERENCES,
    AND, OR, NOT, NULL_TOKEN, TRUE_TOKEN, FALSE_TOKEN, DISTINCT, ORDER, BY, 
    GROUP, HAVING, LIMIT, OFFSET, JOIN, INNER, LEFT, RIGHT, OUTER, ON,
    UNION, INTERSECT, EXCEPT, AS, CASE, WHEN, THEN, ELSE, END, IF, EXISTS,
    BEGIN, COMMIT, ROLLBACK, TRANSACTION, VACUUM, ANALYZE,
    
    // 数据类型
    INT, VARCHAR, CHAR, TEXT, REAL, BOOLEAN, DATE, TIME, TIMESTAMP,
    
    // 标识符和常量
    IDENTIFIER = 100,    // 标识符
    INTEGER_LITERAL,     // 整数常量
    REAL_LITERAL,        // 实数常量
    STRING_LITERAL,      // 字符串常量
    
    // 运算符
    PLUS = 200,          // +
    MINUS,               // -
    MULTIPLY,            // *
    DIVIDE,              // /
    MODULO,              // %
    EQUAL,               // =
    NOT_EQUAL,           // != 或 <>
    LESS_THAN,           // <
    LESS_EQUAL,          // <=
    GREATER_THAN,        // >
    GREATER_EQUAL,       // >=
    
    // 分隔符
    SEMICOLON = 300,     // ;
    COMMA,               // ,
    LEFT_PAREN,          // (
    RIGHT_PAREN,         // )
    DOT,                 // .
    
    // 特殊标记
    END_OF_FILE = 400,   // 文件结束
    UNKNOWN,             // 未知字符
    ERROR_TOKEN          // 错误标记
};

// Token结构体
struct Token {
    TokenType type;      // 种别码
    std::string value;   // 词素值
    int line;           // 行号
    int column;         // 列号
    
    Token(TokenType t, const std::string& v, int l, int c) 
        : type(t), value(v), line(l), column(c) {}
    
    // 获取Token类型的字符串表示
    std::string getTypeName() const;
    
    // 格式化输出：[种别码,词素值,行号,列号]
    std::string toString() const;
};

// 词法分析器类
class Lexer {
public:
    explicit Lexer(const std::string& input);
    
    // 获取下一个Token
    Token nextToken();
    
    // 获取所有Token
    std::vector<Token> tokenize();
    
    // 检查是否到达输入末尾
    bool isAtEnd() const;
    
    // 获取当前位置信息
    int getCurrentLine() const { return current_line_; }
    int getCurrentColumn() const { return current_column_; }
    
    // 获取错误信息
    const std::vector<std::string>& getErrors() const { return errors_; }
    
private:
    std::string input_;
    size_t current_;
    int current_line_;
    int current_column_;
    std::vector<std::string> errors_;
    
    // 关键字映射表
    static std::unordered_map<std::string, TokenType> keywords_;
    
    // 初始化关键字映射表
    static void initializeKeywords();
    
    // 字符处理函数
    char advance();
    char peek() const;
    char peekNext() const;
    bool match(char expected);
    
    // 跳过空白字符
    void skipWhitespace();
    
    // Token识别函数
    Token makeToken(TokenType type, const std::string& value);
    Token makeErrorToken(const std::string& message);
    Token scanString();
    Token scanNumber();
    Token scanIdentifier();
    Token scanOperator();
    
    // 辅助函数
    bool isAlpha(char c) const;
    bool isDigit(char c) const;
    bool isAlphaNumeric(char c) const;
    
    // 错误处理
    void addError(const std::string& message);
};