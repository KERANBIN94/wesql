#include "transaction_manager.h"
#include "../storage/storage_engine.h"

int TransactionManager::start_transaction() {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    int tx_id = next_tx_id++;
    active_txs[tx_id] = true;
    tx_cids[tx_id] = 0;
    tx_locks_[tx_id] = {};
    return tx_id;
}

void TransactionManager::commit(int tx_id) {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    if (active_txs.find(tx_id) == active_txs.end()) {
        return; // Transaction not active
    }
    
    storage_engine_->write_wal(tx_id, "COMMIT", "");
    for (const auto& table_name : tx_locks_[tx_id]) {
        lock_manager_.unlock_table(tx_id, table_name);
    }
    tx_locks_.erase(tx_id);
    active_txs.erase(tx_id);
    committed_txs[tx_id] = next_tx_id;  // Approx commit seq
}

void TransactionManager::rollback(int tx_id) {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    if (active_txs.find(tx_id) == active_txs.end()) {
        return; // Transaction not active
    }
    
    storage_engine_->write_wal(tx_id, "ROLLBACK", "");
    for (const auto& table_name : tx_locks_[tx_id]) {
        lock_manager_.unlock_table(tx_id, table_name);
    }
    tx_locks_.erase(tx_id);
    active_txs.erase(tx_id);
    tx_cids.erase(tx_id);
    aborted_txs[tx_id] = true;
}

bool TransactionManager::is_committed(int tx_id) const {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    return committed_txs.count(tx_id);
}

std::map<int, int> TransactionManager::get_snapshot(int tx_id) {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    std::map<int, int> snapshot = committed_txs;
    for (auto& active : active_txs) {
        snapshot[active.first] = -1;  // Mark active
    }
    return snapshot;
}

int TransactionManager::get_next_cid(int tx_id) {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    return tx_cids[tx_id]++;
}

int TransactionManager::get_current_tx_id() const {
    return next_tx_id - 1;
}

bool TransactionManager::is_aborted(int tx_id) const {
    std::lock_guard<std::mutex> lock(tx_mutex_);
    return aborted_txs.count(tx_id);
}

bool TransactionManager::lock_table(int tx_id, const std::string& table_name, LockMode mode) {
    if (lock_manager_.lock_table(tx_id, table_name, mode)) {
        std::lock_guard<std::mutex> lock(tx_mutex_);
        tx_locks_[tx_id].push_back(table_name);
        return true;
    }
    return false;
}
