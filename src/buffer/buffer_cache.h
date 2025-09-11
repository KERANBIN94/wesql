#ifndef BUFFER_CACHE_H
#define BUFFER_CACHE_H

#include <unordered_map>
#include <list>
#include <string>
#include <mutex>

struct Page;  // Forward declare

class BufferCache {
public:
    BufferCache(size_t capacity);
    Page* get_page(const std::string& file, int page_id);
    void put_page(const std::string& file, int page_id, Page* page);
    void flush_all();

private:
    size_t capacity;
    std::unordered_map<std::string, std::list<std::pair<int, Page*>>::iterator> cache_map;
    std::list<std::pair<int, Page*>> lru_list;
    std::mutex mutex;

    void evict();
};

#endif