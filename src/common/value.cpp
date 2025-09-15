#include "value.h"

std::string to_string(const Value& val) {
    if (val.is_null()) {
        return "NULL";
    }
    if (val.type == DataType::INT) {
        return std::to_string(val.int_value);
    } else if (val.type == DataType::STRING) {
        return val.str_value;
    }
    return "";
}
