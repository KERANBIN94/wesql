#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include <vector>
#include <string>
#include <map>

struct Tid { 
    std::string file; 
    int page_id; 
    short offset; 
};

class BPlusTree {
public:
    void insert(const std::string& key, Tid tid);
    std::vector<Tid> search(const std::string& key) const;

private:
    std::map<std::string, std::vector<Tid>> tree;  // Simplified as map, not real B+
};

#endif