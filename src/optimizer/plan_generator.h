#ifndef PLAN_GENERATOR_H
#define PLAN_GENERATOR_H

#include "../parser/sql_parser.h"
#include <memory>

enum class LogicalOperatorType {
    SEQ_SCAN,
    FILTER,
    PROJECTION,
    INSERT,
    UPDATE,
    DELETE,
    CREATE_TABLE,
    CREATE_INDEX,
    DROP_TABLE,
    DROP_INDEX
};

class LogicalPlanNode {
public:
    LogicalOperatorType type;
    std::vector<std::shared_ptr<LogicalPlanNode>> children;
    // Specific operator fields
    std::string table_name;
    std::string index_name; // For CREATE/DROP INDEX
    std::string index_column; // For CREATE INDEX
    std::vector<WhereCondition> conditions;
    std::vector<ColumnDefinition> columns;
    std::vector<Value> values; // Single-row INSERT
    std::vector<std::vector<Value>> multi_values; // Multi-row INSERT
    std::vector<std::string> projection_columns;
    std::map<std::string, Value> set_clause; // For UPDATE statements

    LogicalPlanNode(LogicalOperatorType type) : type(type) {}
};

class PlanGenerator {
public:
    std::shared_ptr<LogicalPlanNode> create_plan(const ASTNode& ast);
};

void print_logical_plan(const std::shared_ptr<LogicalPlanNode>& node, int indent = 0);

#endif