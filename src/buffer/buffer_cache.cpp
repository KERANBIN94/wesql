#include "buffer_cache.h"
#include "../storage/storage_engine.h"  // For Page
#include <stdexcept>

BufferCache::BufferCache(size_t capacity) : capacity(capacity) {}

Page* BufferCache::get_page(const std::string& file, int page_id) {
    std::lock_guard<std::mutex> lock(mutex);
    std::string key = file + "_" + std::to_string(page_id);
    auto it = cache_map.find(key);
    if (it != cache_map.end()) {
        // Move to front (MRU)
        lru_list.splice(lru_list.begin(), lru_list, it->second);
        return it->second->second;
    }
    // Load from disk
    Page* page = new Page();  // Allocate new
    // Assume StorageEngine has a static load function, but for simplicity:
    // In real: read_page_from_file(file, page_id, *page);
    if (lru_list.size() >= capacity) evict();
    lru_list.emplace_front(page_id, page);
    cache_map[key] = lru_list.begin();
    return page;
}

void BufferCache::put_page(const std::string& file, int page_id, Page* page) {
    // Similar to get, but update content
    get_page(file, page_id);  // Ensure in cache
    // Update page content (caller does)
}

void BufferCache::evict() {
    auto last = lru_list.back();
    std::string key = /* file from somewhere */ + "_" + std::to_string(last.first);
    // Write back to disk if dirty (simplified: assume always write)
    // write_page_to_file(file, last.second, last.first);
    delete last.second;
    cache_map.erase(key);
    lru_list.pop_back();
}

void BufferCache::flush_all() {
    // Write all dirty pages
}