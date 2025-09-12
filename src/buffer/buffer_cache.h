#ifndef BUFFER_CACHE_H
#define BUFFER_CACHE_H

#include <unordered_map>
#include <list>
#include <string>
#include <mutex>

struct Page;  // Forward declare
class StorageEngine;

class BufferCache {
public:
    BufferCache(size_t capacity);
    void set_storage_engine(StorageEngine* storage_engine);
    Page* get_page(const std::string& file, int page_id);
    void put_page(const std::string& file, int page_id, Page* page);
    void flush_all();

private:
    size_t capacity;
    StorageEngine* storage_engine_ = nullptr;
    std::unordered_map<std::string, std::list<std::pair<std::string, Page*>>::iterator> cache_map;
    std::list<std::pair<std::string, Page*>> lru_list;
    std::mutex mutex;

    void evict();
};

#endif