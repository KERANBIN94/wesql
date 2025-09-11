#include "optimizer.h"

ASTNode Optimizer::optimize(const ASTNode& ast, StorageEngine& storage) {
    ASTNode opt_ast = ast;
    if (ast.type == "SELECT") {
        auto it = ast.params.find("where");
        if (it != ast.params.end() && !it->second.empty()) {
            const std::string& where_clause = it->second;
            size_t eq_pos = where_clause.find('=');
            if (eq_pos != std::string::npos) {
                std::string col = where_clause.substr(0, eq_pos);
                std::string val = where_clause.substr(eq_pos + 1);
                
                auto table_it = ast.params.find("table");
                if (table_it != ast.params.end()) {
                    if (storage.has_index(table_it->second, col)) {
                        opt_ast.params["scan_type"] = "index";
                        opt_ast.params["index_col"] = col;
                        opt_ast.params["index_val"] = val;
                    } else {
                        opt_ast.params["scan_type"] = "seq";
                    }
                }
            }
        }
    }
    return opt_ast;
}