#ifndef BUFFER_CACHE_H
#define BUFFER_CACHE_H

#include <unordered_map>
#include <list>
#include <string>
#include <mutex>
#include <utility>
#include <memory>
#include "../common/page.h"

class StorageEngine;

class BufferCache {
public:
    BufferCache(size_t capacity);
    ~BufferCache(); // Add destructor to clean up resources
    void set_storage_engine(StorageEngine* storage_engine);
    Page* get_page(const std::string& file, int page_id);
    void put_page(const std::string& file, int page_id, Page* page);
    void flush_all();
    void print_stats();

private:
    size_t capacity;
    StorageEngine* storage_engine_ = nullptr;
    std::unordered_map<std::string, std::list<std::pair<std::string, std::unique_ptr<Page>>>::iterator> cache_map;
    std::list<std::pair<std::string, std::unique_ptr<Page>>> lru_list;
    std::mutex mutex;

    size_t hits_ = 0;
    size_t misses_ = 0;
    size_t evictions_ = 0;

    void evict();
};

#endif