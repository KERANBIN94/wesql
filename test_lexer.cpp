#include "src/parser/lexer.h"
#include <iostream>
#include <vector>

void testLexer(const std::string& sql, const std::string& description) {
    std::cout << "\n=== " << description << " ===" << std::endl;
    std::cout << "SQL: " << sql << std::endl;
    std::cout << "Tokens:" << std::endl;
    
    Lexer lexer(sql);
    std::vector<Token> tokens = lexer.tokenize();
    
    for (const auto& token : tokens) {
        std::cout << "  " << token.toString() << " (" << token.getTypeName() << ")" << std::endl;
    }
    
    // 显示错误信息
    const auto& errors = lexer.getErrors();
    if (!errors.empty()) {
        std::cout << "Errors:" << std::endl;
        for (const auto& error : errors) {
            std::cout << "  " << error << std::endl;
        }
    }
}

int main() {
    std::cout << "=== SQL词法分析器测试 ===" << std::endl;
    
    // 测试1: 基本SELECT语句
    testLexer("SELECT id, name FROM users WHERE age > 18;", 
              "基本SELECT语句");
    
    // 测试2: 包含字符串字面量
    testLexer("INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');", 
              "包含字符串字面量的INSERT语句");
    
    // 测试3: 包含数字和运算符
    testLexer("UPDATE products SET price = 99.99, quantity = quantity - 1 WHERE id = 123;", 
              "包含数字和运算符的UPDATE语句");
    
    // 测试4: 复杂查询
    testLexer("SELECT u.name, COUNT(*) as order_count FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id HAVING COUNT(*) >= 5;", 
              "复杂的JOIN查询");
    
    // 测试5: CREATE TABLE语句
    testLexer("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price REAL, created_at TIMESTAMP);", 
              "CREATE TABLE语句");
    
    // 测试6: 包含注释和特殊字符
    testLexer("SELECT * FROM table1 WHERE column1 <> 'test' AND column2 <= 100.5;", 
              "包含不等于运算符的查询");
    
    // 测试7: 错误情况 - 未闭合的字符串
    testLexer("SELECT 'unclosed string FROM table1;", 
              "错误测试 - 未闭合的字符串");
    
    // 测试8: 错误情况 - 非法字符
    testLexer("SELECT @ FROM table1;", 
              "错误测试 - 非法字符");
    
    // 测试9: 多行SQL
    testLexer("SELECT id,\n       name,\n       email\nFROM users\nWHERE active = TRUE;", 
              "多行SQL语句");
    
    // 测试10: 转义字符
    testLexer("INSERT INTO messages (content) VALUES ('He said: \\'Hello World\\'');", 
              "包含转义字符的字符串");
    
    return 0;
}