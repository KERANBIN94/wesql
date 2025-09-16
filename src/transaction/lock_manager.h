#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include <string>
#include <unordered_map>
#include <mutex>
#include <vector>

enum class LockMode {
    SHARED,
    EXCLUSIVE
};

class LockManager {
public:
    bool lock_table(int tx_id, const std::string& table_name, LockMode mode);
    void unlock_table(int tx_id, const std::string& table_name);

private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::pair<LockMode, std::vector<int>>> locks_;
};

#endif