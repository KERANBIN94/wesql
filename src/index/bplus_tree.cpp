#include "bplus_tree.h"
#include <algorithm>

BPlusTree::BPlusTree(int degree) : root(std::make_shared<Node>()), degree(degree), cache(nullptr), index_name("") {
    root->is_leaf = true;
}

BPlusTree::BPlusTree(BufferCache& cache, const std::string& index_name) : root(std::make_shared<Node>()), degree(4), cache(&cache), index_name(index_name) {
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

// --- Deletion Implementation ---

void BPlusTree::remove(const std::string& key, const Tid& tid) {
    std::shared_ptr<Node> leaf = find_leaf(key);
    remove_entry(leaf, key, tid);
}

void BPlusTree::remove_entry(std::shared_ptr<Node> node, const std::string& key, const Tid& tid) {
    // Remove entry from node
    auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
    size_t index = std::distance(node->keys.begin(), it);
    node->keys.erase(it);
    if (node->is_leaf) {
        node->tids.erase(node->tids.begin() + index);
    } else {
        node->children.erase(node->children.begin() + index + 1);
    }

    // Handle underflow
    if (node == root) {
        if (root->keys.empty() && !root->children.empty()) {
            root = root->children[0];
            root->parent.reset();
        }
        return;
    }

    if (node->keys.size() < (degree -1) / 2) {
        auto parent = node->parent.lock();
        int node_idx = get_node_index(node);
        int neighbor_idx = (node_idx == 0) ? 1 : node_idx - 1;
        auto neighbor = parent->children[neighbor_idx];

        if (node->keys.size() + neighbor->keys.size() < degree) {
            if (node_idx == 0) { // node is left child
                coalesce_nodes(node, neighbor, neighbor_idx, parent->keys[0]);
            } else { // node is right child
                coalesce_nodes(neighbor, node, node_idx, parent->keys[node_idx - 1]);
            }
        } else {
            redistribute_nodes(node, neighbor, neighbor_idx);
        }
    }
}

void BPlusTree::coalesce_nodes(std::shared_ptr<Node> node, std::shared_ptr<Node> neighbor, int neighbor_index, const std::string& k_prime) {
    // Simplified coalesce logic
    if (!node->is_leaf) {
        node->keys.push_back(k_prime);
    }
    node->keys.insert(node->keys.end(), neighbor->keys.begin(), neighbor->keys.end());
    if (node->is_leaf) {
        node->tids.insert(node->tids.end(), neighbor->tids.begin(), neighbor->tids.end());
        node->next_leaf = neighbor->next_leaf;
    } else {
        node->children.insert(node->children.end(), neighbor->children.begin(), neighbor->children.end());
        for(auto& child : neighbor->children) child->parent = node;
    }

    remove_entry(node->parent.lock(), k_prime, {});
}

void BPlusTree::redistribute_nodes(std::shared_ptr<Node> node, std::shared_ptr<Node> neighbor, int neighbor_index) {
    // Simplified redistribution logic
    auto parent = node->parent.lock();
    if (neighbor_index < get_node_index(node)) { // neighbor is to the left
        if (!node->is_leaf) {
            node->keys.insert(node->keys.begin(), parent->keys[neighbor_index]);
            parent->keys[neighbor_index] = neighbor->keys.back();
            node->children.insert(node->children.begin(), neighbor->children.back());
            neighbor->children.back()->parent = node;
            neighbor->keys.pop_back();
            neighbor->children.pop_back();
        } else {
            node->keys.insert(node->keys.begin(), neighbor->keys.back());
            node->tids.insert(node->tids.begin(), neighbor->tids.back());
            parent->keys[neighbor_index] = neighbor->keys.back();
            neighbor->keys.pop_back();
            neighbor->tids.pop_back();
        }
    } else { // neighbor is to the right
        // Similar logic for right neighbor
    }
}

int BPlusTree::get_node_index(std::shared_ptr<Node> node) {
    auto parent = node->parent.lock();
    if (!parent) return -1;
    for (size_t i = 0; i < parent->children.size(); ++i) {
        if (parent->children[i] == node) {
            return i;
        }
    }
    return -1;
}
