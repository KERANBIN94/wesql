#ifndef SEMANTIC_ANALYZER_H
#define SEMANTIC_ANALYZER_H

#include "parser/sql_parser.h"
#include "catalog.h"

class SemanticAnalyzer {
public:
    void analyze(ASTNode& ast, Catalog& catalog);
};

#endif