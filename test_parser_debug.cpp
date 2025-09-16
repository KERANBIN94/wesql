#include <iostream>
#include <string>
#include <vector>

// 简化的token解析测试
void test_create_table_parsing() {
    std::string sql = "CREATE TABLE temp_users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50));";
    std::cout << "测试SQL: " << sql << std::endl;
    
    // 模拟token化
    std::vector<std::string> tokens = {
        "CREATE", "TABLE", "temp_users", "(", 
        "id", "INT", "AUTO_INCREMENT", "PRIMARY", "KEY", ",",
        "name", "VARCHAR", "(", "50", ")", 
        ")"
    };
    
    std::cout << "\nTokens:" << std::endl;
    for (size_t i = 0; i < tokens.size(); ++i) {
        std::cout << i << ": '" << tokens[i] << "'" << std::endl;
    }
    
    // 模拟解析过程
    std::cout << "\n解析过程:" << std::endl;
    size_t pos = 0;
    
    // CREATE TABLE
    std::cout << "1. " << tokens[pos++] << " " << tokens[pos++] << " " << tokens[pos++] << std::endl;
    
    // (
    std::cout << "2. " << tokens[pos++] << std::endl;
    
    // 第一列: id INT AUTO_INCREMENT PRIMARY KEY
    std::cout << "3. 列名: " << tokens[pos++] << std::endl;
    std::cout << "4. 类型: " << tokens[pos++] << std::endl;
    std::cout << "5. 约束1: " << tokens[pos++] << std::endl;
    std::cout << "6. 约束2: " << tokens[pos++] << " " << tokens[pos++] << std::endl;
    
    // ,
    std::cout << "7. " << tokens[pos++] << std::endl;
    
    // 第二列: name VARCHAR(50)
    std::cout << "8. 列名: " << tokens[pos++] << std::endl;
    std::cout << "9. 类型: " << tokens[pos++] << std::endl;
    std::cout << "10. 长度: " << tokens[pos++] << tokens[pos++] << tokens[pos++] << std::endl;
    
    // )
    std::cout << "11. " << tokens[pos++] << std::endl;
    
    std::cout << "\n解析应该成功！" << std::endl;
}

int main() {
    test_create_table_parsing();
    return 0;
}