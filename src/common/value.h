#ifndef VALUE_H
#define VALUE_H

#include <string>
#include <stdexcept>

enum class DataType {
    INT,
    STRING,
    NULL_TYPE
};

struct Value {
    DataType type;
    int int_value;
    std::string str_value;

    Value() : type(DataType::NULL_TYPE), int_value(0) {}
    Value(int val) : type(DataType::INT), int_value(val) {}
    Value(const std::string& val) : type(DataType::STRING), str_value(val) {}

    bool is_null() const { return type == DataType::NULL_TYPE; }
};

std::string to_string(const Value& val);

#endif