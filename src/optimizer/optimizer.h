#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "../parser/sql_parser.h"
#include "../storage/storage_engine.h"

class Optimizer {
public:
    ASTNode optimize(const ASTNode& ast, StorageEngine& storage);
};

#endif