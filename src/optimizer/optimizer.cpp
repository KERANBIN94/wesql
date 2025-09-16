#include "optimizer.h"

std::shared_ptr<LogicalPlanNode> Optimizer::optimize(ASTNode& ast) {
    // First, run semantic analysis
    semantic_analyzer_.analyze(ast, catalog_);

    // Second, create a logical plan
    auto logical_plan = plan_generator_.create_plan(ast);

    // Third, optimize the logical plan (placeholder)

    return logical_plan;
}