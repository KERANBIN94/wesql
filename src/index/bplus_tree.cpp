#include "bplus_tree.h"

void BPlusTree::insert(const std::string& key, Tid tid) {
    tree[key].push_back(tid);
}

std::vector<Tid> BPlusTree::search(const std::string& key) {
    auto it = tree.find(key);
    if (it != tree.end()) return it->second;
    return {};
}