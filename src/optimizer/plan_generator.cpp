#include "plan_generator.h"
#include <iostream>

std::shared_ptr<LogicalPlanNode> PlanGenerator::create_plan(const ASTNode& ast) {
    if (ast.type == "SELECT") {
        auto scan_node = std::make_shared<LogicalPlanNode>(LogicalOperatorType::SEQ_SCAN);
        scan_node->table_name = ast.table_name;

        std::shared_ptr<LogicalPlanNode> current_node = scan_node;

        if (!ast.where_conditions.empty()) {
            auto filter_node = std::make_shared<LogicalPlanNode>(LogicalOperatorType::FILTER);
            filter_node->conditions = ast.where_conditions;
            filter_node->children.push_back(current_node);
            current_node = filter_node;
        }

        auto projection_node = std::make_shared<LogicalPlanNode>(LogicalOperatorType::PROJECTION);
        for (const auto& col : ast.columns) {
            projection_node->projection_columns.push_back(col.name);
        }
        projection_node->children.push_back(current_node);
        current_node = projection_node;

        return current_node;
    } else if (ast.type == "INSERT") {
        auto insert_node = std::make_shared<LogicalPlanNode>(LogicalOperatorType::INSERT);
        insert_node->table_name = ast.table_name;
        insert_node->values = ast.values;
        return insert_node;
    } else if (ast.type == "CREATE_TABLE") {
        auto create_table_node = std::make_shared<LogicalPlanNode>(LogicalOperatorType::CREATE_TABLE);
        create_table_node->table_name = ast.table_name;
        create_table_node->columns = ast.columns;
        return create_table_node;
    }
    throw std::runtime_error("Unsupported statement type for plan generation: " + ast.type);
}

void print_logical_plan(const std::shared_ptr<LogicalPlanNode>& node, int indent) {
    if (!node) return;
    std::string indentation(indent * 2, ' ');
    switch (node->type) {
        case LogicalOperatorType::SEQ_SCAN:
            std::cout << indentation << "SeqScan: " << node->table_name << std::endl;
            break;
        case LogicalOperatorType::FILTER:
            std::cout << indentation << "Filter: " << std::endl;
            for (const auto& cond : node->conditions) {
                std::cout << indentation << "  " << cond.column << " " << cond.op << " " << to_string(cond.value) << std::endl;
            }
            break;
        case LogicalOperatorType::PROJECTION:
            std::cout << indentation << "Projection: " << std::endl;
            for (const auto& col : node->projection_columns) {
                std::cout << indentation << "  " << col << std::endl;
            }
            break;
        case LogicalOperatorType::INSERT:
            std::cout << indentation << "Insert: " << node->table_name << std::endl;
            break;
        case LogicalOperatorType::CREATE_TABLE:
            std::cout << indentation << "CreateTable: " << node->table_name << std::endl;
            break;
        default:
            std::cout << indentation << "Unknown operator" << std::endl;
    }
    for (const auto& child : node->children) {
        print_logical_plan(child, indent + 1);
    }
}
