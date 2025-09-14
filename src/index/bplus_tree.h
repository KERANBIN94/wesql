#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include <vector>
#include <string>
#include <memory>

struct Tid {
    std::string file;
    int page_id;
    short offset;
};

class BPlusTree {
private:
    struct Node {
        bool is_leaf = false;
        std::vector<std::string> keys;
        std::vector<std::shared_ptr<Node>> children; // For internal nodes
        std::vector<Tid> tids; // For leaf nodes
        std::shared_ptr<Node> next_leaf; // For leaf nodes to link to the next leaf
        std::weak_ptr<Node> parent;
    };

    std::shared_ptr<Node> root;
    int degree;

    // Insertion helpers
    std::shared_ptr<Node> find_leaf(const std::string& key);
    void insert_into_leaf(std::shared_ptr<Node> leaf, const std::string& key, Tid tid);
    void split_leaf(std::shared_ptr<Node> leaf);
    void insert_into_parent(std::shared_ptr<Node> left, const std::string& key, std::shared_ptr<Node> right);
    void split_internal(std::shared_ptr<Node> node);

    // Deletion helpers
    void remove_entry(std::shared_ptr<Node> node, const std::string& key, const Tid& tid);
    void coalesce_nodes(std::shared_ptr<Node> node, std::shared_ptr<Node> neighbor, int neighbor_index, const std::string& k_prime);
    void redistribute_nodes(std::shared_ptr<Node> node, std::shared_ptr<Node> neighbor, int neighbor_index);
    int get_node_index(std::shared_ptr<Node> node);

public:
    BPlusTree(int degree = 4);
    void insert(const std::string& key, Tid tid);
    void remove(const std::string& key, const Tid& tid);
    std::vector<Tid> search(const std::string& key);
    std::vector<Tid> search_range(const std::string& start_key, const std::string& end_key);
};

#endif
