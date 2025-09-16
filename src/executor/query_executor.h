#ifndef QUERY_EXECUTOR_H
#define QUERY_EXECUTOR_H

#include "../optimizer/plan_generator.h"
#include "../storage/storage_engine.h"
#include "../transaction/transaction_manager.h"
#include <map>
#include <vector>

struct ResultSet {
    std::vector<std::string> columns;
    std::vector<std::vector<Value>> rows;
};

ResultSet execute_plan(std::shared_ptr<LogicalPlanNode> plan, StorageEngine& storage, TransactionManager& tx_manager, int tx_id, const std::map<int, int>& snapshot);

#endif