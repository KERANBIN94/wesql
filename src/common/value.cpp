#include "value.h"

std::string to_string(const Value& val) {
    switch (val.type) {
        case DataType::INT:
            return std::to_string(val.int_value);
        case DataType::STRING:
            return val.str_value;
    }
    return "";
}
