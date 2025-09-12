#include "storage_engine.h"
#include "../index/bplus_tree.h"
#include <stdexcept>
#include <cstring>
#include <filesystem>

// Removed misplaced create_directory and using namespace directive
StorageEngine::StorageEngine(BufferCache& cache) : cache(cache), wal_log("wal.log", std::ios::app) {
    std::filesystem::create_directory("data");
}

void StorageEngine::create_table(const std::string& table_name, const std::vector<std::string>& columns) {
    metadata[table_name] = columns;
    files[table_name] = {"data/" + table_name + "_0.dat"};
    std::ofstream file(files[table_name][0], std::ios::binary);
    file.close();
}

void StorageEngine::create_index(const std::string& table_name, const std::string& column) {
    std::string key = table_name + "_" + column;
    indexes[key] = std::make_unique<BPlusTree>();
}

void StorageEngine::insert_record(const std::string& table_name, const Record& record, int tx_id, int cid) {
    write_wal("INSERT", table_name + "|" + std::to_string(tx_id));
    // Find file and page (simplified: last file, last page)
    std::string file = files[table_name].back();
    int page_id = 0;  // Assume single page for simplicity, extend to find free space
    Page* page = cache.get_page(file, page_id);
    page->dirty = true;

    // Add item pointer
    ItemPointer ip;
    size_t record_size = sizeof(Record);
    ip.offset = page->header.pd_upper - record_size;
    ip.length = record_size;
    page->item_pointers.push_back(ip);

    // Copy data
    memcpy(&page->data[ip.offset], &record, record_size);

    page->header.pd_lower += sizeof(ItemPointer);
    page->header.pd_upper -= ip.length;

    // Update index if exists
    for (auto& idx : indexes) {
        const std::string& index_key = idx.first;
        if (index_key.rfind(table_name + "_", 0) == 0) { // index is for this table
            std::string column_name = index_key.substr(table_name.length() + 1);
            
            // Find column index from metadata
            const auto& table_columns = metadata.at(table_name);
            int column_index = -1;
            for (size_t i = 0; i < table_columns.size(); ++i) {
                if (table_columns[i] == column_name) {
                    column_index = i;
                    break;
                }
            }

            if (column_index != -1) {
                // NOTE: This assumes `record.columns` is accessible and correct.
                // Proper serialization of `Record` is needed if it contains non-POD types.
                idx.second->insert(record.columns[column_index], {file, page_id, (short)ip.offset});
            }
        }
    }
}

std::vector<Record> StorageEngine::scan_table(const std::string& table_name, int tx_id, int cid, const std::map<int, int>& snapshot) {
    std::vector<Record> result;
    for (const auto& file : files[table_name]) {
        int page_id = 0;  // Simplified
        Page* page = cache.get_page(file, page_id);
        for (const auto& ip : page->item_pointers) {
            Record rec;
            memcpy(&rec, &page->data[ip.offset], ip.length);
            if (is_visible(rec, tx_id, cid, snapshot)) {
                result.push_back(rec);
            }
        }
    }
    return result;
}

std::vector<Record> StorageEngine::index_scan(const std::string& table_name, const std::string& column, const std::string& value, int tx_id, int cid, const std::map<int, int>& snapshot) {
    std::vector<Record> result;
    std::string key = table_name + "_" + column;
    auto it = indexes.find(key);
    if (it != indexes.end()) {
        auto tids = it->second->search(value);
        for (auto tid : tids) {
            Page* page = cache.get_page(tid.file, tid.page_id);
            Record rec;
            memcpy(&rec, &page->data[tid.offset], sizeof(Record));
            if (is_visible(rec, tx_id, cid, snapshot)) result.push_back(rec);
        }
    }
    return result;
}

bool StorageEngine::has_index(const std::string& table_name, const std::string& column) const {
    std::string key = table_name + "_" + column;
    return indexes.count(key);
}

// Similar updates for update/delete: set xmax, insert new version for update

bool StorageEngine::is_visible(const Record& rec, int tx_id, int cid, const std::map<int, int>& snapshot) {
    // Full MVCC visibility (based on PostgreSQL rules)
    if (rec.xmax != 0) {
        if (snapshot.count(rec.xmax) == 0) { // Not in snapshot means committed
            if (rec.xmax < tx_id) return false; // Committed delete/update before we started
        } else { // In snapshot means active
            return false; // Deleted by an active transaction
        }
    }
    if (snapshot.count(rec.xmin)) { // Created by an active transaction
        if (rec.xmin != tx_id) return false; // From another active transaction
        if (rec.cid > cid) return false; // Created by a later command in same transaction
    } else { // Created by a committed transaction
        if (rec.xmin > tx_id) return false; // Created after we started
    }
    return true;
}

void StorageEngine::write_page_to_file(const std::string& file, const Page& page, int page_id) {
    std::ofstream out(file, std::ios::binary | std::ios::in | std::ios::out);
    out.seekp(page_id * PAGE_SIZE);
    out.write(reinterpret_cast<const char*>(&page), PAGE_SIZE);
    out.close();
}

void StorageEngine::read_page_from_file(const std::string& file, int page_id, Page& page) {
    std::ifstream in(file, std::ios::binary);
    in.seekg(page_id * PAGE_SIZE);
    in.read(reinterpret_cast<char*>(&page), PAGE_SIZE);
    in.close();
}

void StorageEngine::write_wal(const std::string& operation, const std::string& data) {
    wal_log << operation << ":" << data << std::endl;
    wal_log.flush();
}
