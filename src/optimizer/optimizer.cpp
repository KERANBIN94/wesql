#include "optimizer.h"

ASTNode Optimizer::optimize(const ASTNode& ast, StorageEngine& storage) {
    ASTNode opt_ast = ast;
    if (ast.type == "SELECT") {
        opt_ast.hints["scan_type"] = "seq"; // Default to sequential scan
        for (const auto& cond : ast.where_conditions) {
            // Check for simple equality conditions on indexed columns
            if (cond.op == "=") {
                if (storage.has_index(ast.table_name, cond.column)) {
                    opt_ast.hints["scan_type"] = "index";
                    opt_ast.hints["index_col"] = cond.column;
                    opt_ast.hints["index_val"] = to_string(cond.value);
                    break; // Use the first available index
                }
            }
        }
    }
    return opt_ast;
}
