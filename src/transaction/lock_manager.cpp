#include "lock_manager.h"
#include <vector>

bool LockManager::lock_table(int tx_id, const std::string& table_name, LockMode mode) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if this table has any existing locks
    if (locks_.find(table_name) == locks_.end()) {
        // No existing lock, create new one
        locks_[table_name] = {mode, {tx_id}};
        return true;
    }
    
    auto& lock_info = locks_[table_name];

    if (lock_info.first == LockMode::EXCLUSIVE) {
        // Exclusive lock exists
        if (lock_info.second.size() == 1 && lock_info.second[0] == tx_id) {
            // Same transaction already holds exclusive lock
            return true;
        }
        return false;
    } else if (lock_info.first == LockMode::SHARED) {
        // Shared lock exists
        if (mode == LockMode::SHARED) {
            for(int id : lock_info.second) {
                if (id == tx_id) return true; // Already has shared lock
            }
            lock_info.second.push_back(tx_id);
            return true;
        }
        // Trying to get exclusive lock
        if (lock_info.second.size() == 1 && lock_info.second[0] == tx_id) {
            lock_info.first = LockMode::EXCLUSIVE;
            return true;
        }
        return false;
    }

    // This should not happen with explicit initialization
    return false;
}

void LockManager::unlock_table(int tx_id, const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (locks_.find(table_name) == locks_.end()) return;

    auto& lock_info = locks_[table_name];
    if (lock_info.first == LockMode::EXCLUSIVE) {
        if (lock_info.second.size() == 1 && lock_info.second[0] == tx_id) {
            locks_.erase(table_name);
        }
    } else if (lock_info.first == LockMode::SHARED) {
        auto& tx_ids = lock_info.second;
        for (auto it = tx_ids.begin(); it != tx_ids.end(); ++it) {
            if (*it == tx_id) {
                tx_ids.erase(it);
                break;
            }
        }
        if (tx_ids.empty()) {
            locks_.erase(table_name);
        }
    }
}
