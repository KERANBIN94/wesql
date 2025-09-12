#include "bplus_tree.h"
#include <algorithm>

BPlusTree::BPlusTree(int degree) : root(std::make_shared<Node>()), degree(degree) {
    root->is_leaf = true;
}

void BPlusTree::insert(const std::string& key, Tid tid) {
    std::shared_ptr<Node> leaf = find_leaf(key);
    insert_into_leaf(leaf, key, tid);

    if (leaf->keys.size() >= degree) {
        split_leaf(leaf);
    }
}

std::vector<Tid> BPlusTree::search(const std::string& key) {
    std::shared_ptr<Node> leaf = find_leaf(key);
    std::vector<Tid> results;
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    while (it != leaf->keys.end() && *it == key) {
        size_t index = std::distance(leaf->keys.begin(), it);
        results.push_back(leaf->tids[index]);
        ++it;
    }
    return results;
}

std::vector<Tid> BPlusTree::search_range(const std::string& start_key, const std::string& end_key) {
    std::vector<Tid> results;
    std::shared_ptr<Node> leaf = find_leaf(start_key);

    bool done = false;
    while (leaf && !done) {
        for (size_t i = 0; i < leaf->keys.size(); ++i) {
            if (leaf->keys[i] > end_key) {
                done = true;
                break;
            }
            if (leaf->keys[i] >= start_key) {
                results.push_back(leaf->tids[i]);
            }
        }
        leaf = leaf->next_leaf;
    }
    return results;
}

// --- Helper function implementations ---

std::shared_ptr<BPlusTree::Node> BPlusTree::find_leaf(const std::string& key) {
    std::shared_ptr<Node> current = root;
    while (!current->is_leaf) {
        auto it = std::upper_bound(current->keys.begin(), current->keys.end(), key);
        size_t index = std::distance(current->keys.begin(), it);
        current->children[index]->parent = current;
        current = current->children[index];
    }
    return current;
}

void BPlusTree::insert_into_leaf(std::shared_ptr<Node> leaf, const std::string& key, Tid tid) {
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    size_t index = std::distance(leaf->keys.begin(), it);
    leaf->keys.insert(it, key);
    leaf->tids.insert(leaf->tids.begin() + index, tid);
}

void BPlusTree::split_leaf(std::shared_ptr<Node> leaf) {
    auto new_leaf = std::make_shared<Node>();
    new_leaf->is_leaf = true;

    size_t mid_index = degree / 2;

    new_leaf->keys.assign(leaf->keys.begin() + mid_index, leaf->keys.end());
    new_leaf->tids.assign(leaf->tids.begin() + mid_index, leaf->tids.end());

    leaf->keys.resize(mid_index);
    leaf->tids.resize(mid_index);

    new_leaf->next_leaf = leaf->next_leaf;
    leaf->next_leaf = new_leaf;

    std::string key_to_promote = new_leaf->keys[0];
    insert_into_parent(leaf, key_to_promote, new_leaf);
}

void BPlusTree::insert_into_parent(std::shared_ptr<Node> left, const std::string& key, std::shared_ptr<Node> right) {
    std::shared_ptr<Node> parent = left->parent.lock();

    if (!parent) {
        root = std::make_shared<Node>();
        root->keys.push_back(key);
        root->children.push_back(left);
        root->children.push_back(right);
        left->parent = root;
        right->parent = root;
        return;
    }

    auto it = std::upper_bound(parent->keys.begin(), parent->keys.end(), key);
    size_t index = std::distance(parent->keys.begin(), it);
    parent->keys.insert(it, key);
    parent->children.insert(parent->children.begin() + index + 1, right);
    right->parent = parent;

    if (parent->keys.size() >= degree) {
        split_internal(parent);
    }
}

void BPlusTree::split_internal(std::shared_ptr<Node> node) {
    auto new_internal = std::make_shared<Node>();
    
    size_t mid_index = (degree - 1) / 2;
    std::string key_to_promote = node->keys[mid_index];

    new_internal->keys.assign(node->keys.begin() + mid_index + 1, node->keys.end());
    new_internal->children.assign(node->children.begin() + mid_index + 1, node->children.end());
    
    for(auto& child : new_internal->children) {
        child->parent = new_internal;
    }

    node->keys.resize(mid_index);
    node->children.resize(mid_index + 1);

    insert_into_parent(node, key_to_promote, new_internal);
}