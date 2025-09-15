#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "../parser/sql_parser.h"
#include "../storage/storage_engine.h"
#include "semantic_analyzer.h"
#include "catalog.h"
#include "plan_generator.h"

class Optimizer {
public:
    std::shared_ptr<LogicalPlanNode> optimize(ASTNode& ast, StorageEngine& storage);
private:
    SemanticAnalyzer semantic_analyzer_;
    PlanGenerator plan_generator_;
    Catalog catalog_;
};

#endif