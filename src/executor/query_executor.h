#ifndef QUERY_EXECUTOR_H
#define QUERY_EXECUTOR_H

#include "../parser/sql_parser.h"
#include "../storage/storage_engine.h"
#include "../transaction/transaction_manager.h"
#include <map>

void execute_query(const ASTNode& ast, StorageEngine& storage, TransactionManager& tx_manager, int tx_id, const std::map<int, int>& snapshot);

#endif