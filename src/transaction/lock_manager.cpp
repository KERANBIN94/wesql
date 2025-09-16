#include "lock_manager.h"
#include <vector>

bool LockManager::lock_table(int tx_id, const std::string& table_name, LockMode mode) {
    std::lock_guard<std::mutex> lock(mutex_);
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

    // No lock
    lock_info.first = mode;
    lock_info.second.push_back(tx_id);
    return true;
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
        for (auto it = lock_info.second.begin(); it != lock_info.second.end(); ++it) {
            if (*it == tx_id) {
                lock_info.second.erase(it);
                break;
            }
        }
        if (lock_info.second.empty()) {
            locks_.erase(table_name);
        }
    }
}
