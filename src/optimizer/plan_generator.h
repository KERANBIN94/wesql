#ifndef PLAN_GENERATOR_H
#define PLAN_GENERATOR_H

#include "../parser/sql_parser.h"
#include <memory>

enum class LogicalOperatorType {
    SEQ_SCAN,
    FILTER,
    PROJECTION,
    INSERT,
    CREATE_TABLE
};

class LogicalPlanNode {
public:
    LogicalOperatorType type;
    std::vector<std::shared_ptr<LogicalPlanNode>> children;
    // Specific operator fields
    std::string table_name;
    std::vector<WhereCondition> conditions;
    std::vector<ColumnDefinition> columns;
    std::vector<Value> values;
    std::vector<std::string> projection_columns;

    LogicalPlanNode(LogicalOperatorType type) : type(type) {}
};

class PlanGenerator {
public:
    std::shared_ptr<LogicalPlanNode> create_plan(const ASTNode& ast);
};

void print_logical_plan(const std::shared_ptr<LogicalPlanNode>& node, int indent = 0);

#endif