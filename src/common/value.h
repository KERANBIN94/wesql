#include <string>

enum class DataType {
    INT,
    STRING
};

struct Value {
    DataType type;
    int int_value;
    std::string str_value;
};

std::string to_string(const Value& val);