#include "buffer_cache.h"
#include "../storage/storage_engine.h"
#include <stdexcept>
#include <iostream>

BufferCache::BufferCache(size_t capacity) : capacity(capacity) {}

BufferCache::~BufferCache() {
    // Smart pointers will automatically clean up
}

void BufferCache::set_storage_engine(StorageEngine* storage_engine) {
    storage_engine_ = storage_engine;
}

Page* BufferCache::get_page(const std::string& file, int page_id) {
    std::lock_guard<std::mutex> lock(mutex);
    std::string key = file + "_" + std::to_string(page_id);
    auto it = cache_map.find(key);
    if (it != cache_map.end()) {
        hits_++;
        lru_list.splice(lru_list.begin(), lru_list, it->second);
        return it->second->second.get();
    }

    misses_++;
    if (lru_list.size() >= capacity) {
        evict();
    }

    auto page = std::make_unique<Page>();
    Page* page_ptr = page.get();
    if (storage_engine_) {
        storage_engine_->read_page_from_file(file, page_id, *page);
    }

    lru_list.emplace_front(key, std::move(page));
    cache_map[key] = lru_list.begin();
    return page_ptr;
}

void BufferCache::put_page(const std::string& file, int page_id, Page* page) {
    std::lock_guard<std::mutex> lock(mutex);
    std::string key = file + "_" + std::to_string(page_id);
    auto it = cache_map.find(key);
    if (it != cache_map.end()) {
        // Page exists, update its content and mark as dirty.
        *(it->second->second) = *page;
        it->second->second->dirty = true;
        lru_list.splice(lru_list.begin(), lru_list, it->second);
        // No need to delete - caller owns the page
    } else {
        // Page does not exist, insert it.
        if (lru_list.size() >= capacity) {
            evict();
        }
        auto new_page = std::make_unique<Page>(*page);
        new_page->dirty = true;
        lru_list.emplace_front(key, std::move(new_page));
        cache_map[key] = lru_list.begin();
    }
}

void BufferCache::flush_all() {
    std::lock_guard<std::mutex> lock(mutex);
    for (auto& pair : lru_list) {
        Page* page = pair.second.get();
        if (page->dirty && storage_engine_) {
            const std::string& key = pair.first;
            size_t separator_pos = key.find_last_of('_');
            if (separator_pos != std::string::npos) {
                std::string file = key.substr(0, separator_pos);
                int page_id = std::stoi(key.substr(separator_pos + 1));
                storage_engine_->write_page_to_file(file, *page, page_id);
                page->dirty = false;
            }
        }
    }
}

void BufferCache::print_stats() {
    std::cout << "Cache Stats: Hits=" << hits_ << ", Misses=" << misses_ << ", Evictions=" << evictions_ << std::endl;
}

void BufferCache::evict() {
    if (lru_list.empty()) {
        return;
    }
    evictions_++;
    auto& last = lru_list.back();
    std::string key = last.first;
    Page* page = last.second.get();

    if (page->dirty && storage_engine_) {
        size_t separator_pos = key.find_last_of('_');
        if (separator_pos != std::string::npos) {
            std::string file = key.substr(0, separator_pos);
            int page_id = std::stoi(key.substr(separator_pos + 1));
            storage_engine_->write_page_to_file(file, *page, page_id);
        }
    }

    cache_map.erase(key);
    lru_list.pop_back();
    // No need to delete - unique_ptr handles cleanup automatically
}
