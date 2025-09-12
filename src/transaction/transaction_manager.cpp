#include "transaction_manager.h"

int TransactionManager::start_transaction() {
    int tx_id = next_tx_id++;
    active_txs[tx_id] = true;
    tx_cids[tx_id] = 0;
    return tx_id;
}

void TransactionManager::commit(int tx_id) {
    active_txs.erase(tx_id);
    committed_txs[tx_id] = next_tx_id.load();  // Approx commit seq
}

void TransactionManager::rollback(int tx_id) {
    active_txs.erase(tx_id);
    tx_cids.erase(tx_id);
}

std::map<int, int> TransactionManager::get_snapshot(int tx_id) {
    std::map<int, int> snapshot = committed_txs;
    for (auto& active : active_txs) {
        snapshot[active.first] = -1;  // Mark active
    }
    return snapshot;
}

int TransactionManager::get_next_cid(int tx_id) {
    return tx_cids[tx_id]++;
}

int TransactionManager::get_current_tx_id() const {
    return next_tx_id - 1;
}