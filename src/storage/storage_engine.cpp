#include "storage_engine.h"
#include "../transaction/transaction_manager.h"
#include <stdexcept>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <algorithm>

// Helper function to write a value to a buffer
char* serialize_value(char* buffer, const Value& value) {
    *reinterpret_cast<DataType*>(buffer) = value.type;
    buffer += sizeof(DataType);
    if (value.type == DataType::INT) {
        *reinterpret_cast<int*>(buffer) = value.int_value;
        buffer += sizeof(int);
    } else if (value.type == DataType::STRING) {
        const std::string& str = value.str_value;
        size_t len = str.length();
        *reinterpret_cast<size_t*>(buffer) = len;
        buffer += sizeof(size_t);
        std::memcpy(buffer, str.c_str(), len);
        buffer += len;
    }
    return buffer;
}

// Helper function to read a value from a buffer
const char* deserialize_value(const char* buffer, Value& value) {
    value.type = *reinterpret_cast<const DataType*>(buffer);
    buffer += sizeof(DataType);
    if (value.type == DataType::INT) {
        value.int_value = *reinterpret_cast<const int*>(buffer);
        buffer += sizeof(int);
    } else if (value.type == DataType::STRING) {
        size_t len = *reinterpret_cast<const size_t*>(buffer);
        buffer += sizeof(size_t);
        value.str_value = std::string(buffer, len);
        buffer += len;
    }
    return buffer;
}


StorageEngine::StorageEngine(BufferCache& cache) : cache(cache) {
    // In a real database, metadata would be loaded from a catalog file.
    // For simplicity, we'll keep it in memory.
    wal_log.open("wal.log", std::ios::app);
}

void StorageEngine::create_table(const std::string& table_name, const std::vector<ColumnDefinition>& columns) {
    if (table_files.count(table_name)) {
        throw std::runtime_error("Table already exists: " + table_name);
    }
    std::string file_path = "data/" + table_name + ".tbl";
    table_files[table_name] = file_path;
    table_page_counts[table_name] = 0;
    
    std::vector<Column> cols;
    for(const auto& col_def : columns) {
        cols.push_back({col_def.name, col_def.type});
    }
    metadata[table_name] = cols;

    // Create an empty file for the table
    std::ofstream table_file(file_path);
    if (!table_file) {
        throw std::runtime_error("Could not create table file: " + file_path);
    }
    table_file.close();
    add_new_page_to_table(table_name);
    write_wal("CREATE_TABLE", table_name);
}

void StorageEngine::create_index(const std::string& table_name, const std::string& column_name) {
    if (!metadata.count(table_name)) {
        throw std::runtime_error("Table not found: " + table_name);
    }
    const auto& cols = metadata.at(table_name);
    auto it = std::find_if(cols.begin(), cols.end(), [&](const Column& col){ return col.name == column_name; });
    if (it == cols.end()) {
        throw std::runtime_error("Column not found: " + column_name);
    }

    std::string index_name = table_name + "_" + column_name + "_idx";
    indexes[index_name] = std::make_unique<BPlusTree>(cache, table_name + "_" + column_name + ".idx");
    
    // Populate the index
    // This is a simplified initial population. A real implementation would be more robust.
    TransactionManager tx_manager;
    auto records = scan_table(table_name, 0, 0, {}, tx_manager); // Simplified call
    int col_idx = std::distance(cols.begin(), it);

    for(size_t i = 0; i < records.size(); ++i) {
        const auto& rec = records[i];
        if(col_idx < rec.columns.size()) {
            // B+Tree needs a string key for now.
            std::string key; // Placeholder
			const auto& col_val = rec.columns[col_idx];
            if (col_val.type == DataType::INT) {
                key = std::to_string(col_val.int_value);
            } else if (col_val.type == DataType::STRING) {
                key = col_val.str_value;
            }
            Tid tid = {table_name, static_cast<int>(i / 100), static_cast<short>(i % 100)}; // Example Tid
            indexes[index_name]->insert(key, tid);
        }
    }
    write_wal("CREATE_INDEX", index_name);
}


void StorageEngine::insert_record(const std::string& table_name, const Record& record, int tx_id, int cid) {
    // Simplified serialization
    char buffer[PAGE_SIZE];
    char* ptr = buffer;
    
    // Serialize header
    *reinterpret_cast<int*>(ptr) = record.xmin; ptr += sizeof(int);
    *reinterpret_cast<int*>(ptr) = record.xmax; ptr += sizeof(int);
    *reinterpret_cast<int*>(ptr) = record.cid; ptr += sizeof(int);
    
    // Serialize columns
    for (const auto& val : record.columns) {
        ptr = serialize_value(ptr, val);
    }
    uint16_t record_size = ptr - buffer;

    int page_id = find_page_with_space(table_name, record_size + sizeof(ItemPointer));
    Page* page = cache.get_page(table_files.at(table_name), page_id);

    // Add record to page data
    page->header.pd_upper -= record_size;
    std::memcpy(page->data + page->header.pd_upper, buffer, record_size);

    // Add item pointer
    page->item_pointers.push_back({page->header.pd_upper, record_size});
    page->header.pd_lower += sizeof(ItemPointer);
    page->dirty = true;

    update_page_free_space(table_name, page_id, page->header.pd_upper - page->header.pd_lower);
}

std::vector<Record> StorageEngine::scan_table(const std::string& table_name, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager) {
    std::vector<Record> result;
    int page_count = table_page_counts.at(table_name);
    for (int i = 0; i < page_count; ++i) {
        Page* page = cache.get_page(table_files.at(table_name), i);
        for (const auto& item_ptr : page->item_pointers) {
            const char* ptr = page->data + item_ptr.offset;
            Record rec;
            
            // Deserialize header
            rec.xmin = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            rec.xmax = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            rec.cid = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);

            // Deserialize columns
            const auto& cols = metadata.at(table_name);
            for (size_t j = 0; j < cols.size(); ++j) {
                Value val;
                ptr = deserialize_value(ptr, val);
                rec.columns.push_back(val);
            }

            if (is_visible(rec, tx_id, cid, snapshot, tx_manager)) {
                result.push_back(rec);
            }
        }
    }
    return result;
}

// ... other methods like index_scan, delete, update need to be updated for typed values ...

void StorageEngine::drop_table(const std::string& table_name) {
    if (table_files.find(table_name) == table_files.end()) {
        throw std::runtime_error("Table not found: " + table_name);
    }
    std::filesystem::remove(table_files.at(table_name));
    metadata.erase(table_name);
    table_files.erase(table_name);
    table_page_counts.erase(table_name);
    free_space_maps.erase(table_name);
    write_wal("DROP_TABLE", table_name);
}

void StorageEngine::drop_index(const std::string& index_name) {
    if (indexes.find(index_name) == indexes.end()) {
        throw std::runtime_error("Index not found: " + index_name);
    }
    indexes.erase(index_name);
    write_wal("DROP_INDEX", index_name);
}

int StorageEngine::add_new_page_to_table(const std::string& table_name) {
    int new_page_id = table_page_counts.at(table_name)++;
    Page new_page;
    new_page.header.pd_lower = sizeof(PageHeader);
    new_page.header.pd_upper = PAGE_SIZE;
    new_page.dirty = true;
    write_page_to_file(table_files.at(table_name), new_page, new_page_id);
    update_page_free_space(table_name, new_page_id, new_page.header.pd_upper - new_page.header.pd_lower);
    return new_page_id;
}

int StorageEngine::find_page_with_space(const std::string& table_name, uint16_t required_space) {
    for(auto const& [page_id, free_space] : free_space_maps.at(table_name)) {
        if (free_space >= required_space) {
            return page_id;
        }
    }
    return add_new_page_to_table(table_name);
}

void StorageEngine::update_page_free_space(const std::string& table_name, int page_id, uint16_t new_free_space) {
    free_space_maps[table_name][page_id] = new_free_space;
}

bool StorageEngine::is_visible(const Record& rec, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager) {
    if (tx_manager.is_aborted(rec.xmin)) {
        return false;
    }
    if (rec.xmin == tx_id) {
        return rec.xmax == 0;
    }
    if (tx_manager.is_committed(rec.xmin) && snapshot.count(rec.xmin)) {
        if (rec.xmax == 0) return true;
        if (rec.xmax == tx_id) return true;
        if (tx_manager.is_aborted(rec.xmax)) return true;
        if (!tx_manager.is_committed(rec.xmax) || !snapshot.count(rec.xmax)) return true;
    }
    return false;
}

void StorageEngine::write_page_to_file(const std::string& file, const Page& page, int page_id) {
    std::fstream fs(file, std::ios::in | std::ios::out | std::ios::binary);
    fs.seekp(page_id * PAGE_SIZE);
    fs.write(reinterpret_cast<const char*>(&page), PAGE_SIZE);
}

void StorageEngine::read_page_from_file(const std::string& file, int page_id, Page& page) {
    std::ifstream fs(file, std::ios::binary);
    fs.seekg(page_id * PAGE_SIZE);
    fs.read(reinterpret_cast<char*>(&page), PAGE_SIZE);
}

const std::vector<Column>& StorageEngine::get_table_metadata(const std::string& table_name) const {
    return metadata.at(table_name);
}

void StorageEngine::write_wal(const std::string& operation, const std::string& data) {
    if (wal_log.is_open()) {
        wal_log << operation << " " << data << std::endl;
    }
}
// Dummy implementations for methods that are not fully implemented yet
int StorageEngine::delete_records(const std::string& table_name, const std::vector<WhereCondition>& conditions, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager) { return 0; }
int StorageEngine::update_records(const std::string& table_name, const std::vector<WhereCondition>& conditions, const std::map<std::string, Value>& set_clause, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager) { return 0; }
std::vector<Record> StorageEngine::index_scan(const std::string& table_name, const std::string& column, const Value& value, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager) { return {}; }
void StorageEngine::vacuum_table(const std::string& table_name, TransactionManager& tx_manager) {}
bool StorageEngine::has_index(const std::string& table_name, const std::string& column) const { return false; }
