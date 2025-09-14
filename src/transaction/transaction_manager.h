#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H

#include <map>
#include <atomic>

class TransactionManager {
public:
    int start_transaction();
    void commit(int tx_id);
    void rollback(int tx_id);
    std::map<int, int> get_snapshot(int tx_id);  // active txs and min tx
    int get_current_tx_id() const;
    int get_next_cid(int tx_id);
    bool is_aborted(int tx_id) const;
    bool is_committed(int tx_id) const;

private:
    std::atomic<int> next_tx_id{1};
    std::map<int, bool> active_txs;
    std::map<int, int> committed_txs;  // tx_id -> commit time
    std::map<int, bool> aborted_txs;
    std::map<int, int> tx_cids;  // tx_id -> current cid
};

#endif