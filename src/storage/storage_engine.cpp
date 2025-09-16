#include "storage_engine.h"
#include "../buffer/buffer_cache.h"
#include "../transaction/transaction_manager.h"
#include <stdexcept>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <algorithm>
#include <set>
#include <iomanip>
#include <sstream>

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
    wal_log.open("wal.log", std::ios::in | std::ios::out | std::ios::app);
    recover_from_wal();
    bootstrap_catalog();
    load_catalog();
}

void StorageEngine::recover_from_wal() {
    wal_log.seekg(0);
    std::string line;
    std::map<int, std::vector<std::string>> tx_logs;
    std::set<int> committed_txs;

    while (std::getline(wal_log, line)) {
        std::stringstream ss(line);
        int tx_id;
        std::string op;
        ss >> tx_id >> op;
        tx_logs[tx_id].push_back(line);
        if (op == "COMMIT") {
            committed_txs.insert(tx_id);
        }
    }

    for (auto const& [tx_id, logs] : tx_logs) {
        if (committed_txs.find(tx_id) == committed_txs.end()) {
            // Undo uncommitted transactions (simplified)
            std::cout << "Rolling back tx " << tx_id << std::endl;
        } else {
            // Redo committed transactions (simplified)
             std::cout << "Redoing tx " << tx_id << std::endl;
        }
    }

    // Clear WAL after recovery
    wal_log.close();
    wal_log.open("wal.log", std::ios::out | std::ios::trunc);
}

void StorageEngine::bootstrap_catalog() {
    std::string sys_tables_path = "data/sys_tables.tbl";
    std::string sys_columns_path = "data/sys_columns.tbl";
    
    if (!std::filesystem::exists(sys_tables_path) || !std::filesystem::exists(sys_columns_path)) {
        // Clear any existing metadata to avoid conflicts
        metadata.clear();
        table_files.clear();
        table_page_counts.clear();
        
        // Create sys_tables and sys_columns
        std::vector<ColumnDefinition> sys_tables_cols = {{"table_name", DataType::STRING}};
        create_table("sys_tables", sys_tables_cols, 0, 0);

        std::vector<ColumnDefinition> sys_columns_cols = {
            {"table_name", DataType::STRING},
            {"column_name", DataType::STRING},
            {"column_type", DataType::INT},
            {"not_null", DataType::INT} // Using INT as bool
        };
        create_table("sys_columns", sys_columns_cols, 0, 0);

        // Manually insert schema for sys_tables and sys_columns
        Record r1; r1.columns = {Value("sys_tables")}; insert_record("sys_tables", r1, 0, 0);
        Record r2; r2.columns = {Value("sys_columns")}; insert_record("sys_tables", r2, 0, 0);

        Record r3; r3.columns = {Value("sys_tables"), Value("table_name"), Value((int)DataType::STRING), Value(1)}; insert_record("sys_columns", r3, 0, 0);
        Record r4; r4.columns = {Value("sys_columns"), Value("table_name"), Value((int)DataType::STRING), Value(1)}; insert_record("sys_columns", r4, 0, 0);
        Record r5; r5.columns = {Value("sys_columns"), Value("column_name"), Value((int)DataType::STRING), Value(1)}; insert_record("sys_columns", r5, 0, 0);
        Record r6; r6.columns = {Value("sys_columns"), Value("column_type"), Value((int)DataType::INT), Value(1)}; insert_record("sys_columns", r6, 0, 0);
        Record r7; r7.columns = {Value("sys_columns"), Value("not_null"), Value((int)DataType::INT), Value(1)}; insert_record("sys_columns", r7, 0, 0);
    }
}

void StorageEngine::load_catalog() {
    // Discover all table files and initialize page counts
    if (std::filesystem::exists("data")) {
        for (const auto& entry : std::filesystem::directory_iterator("data")) {
            if (entry.is_regular_file() && entry.path().extension() == ".tbl") {
                std::string table_name = entry.path().stem().string();
                table_files[table_name] = entry.path().string();
                auto file_size = std::filesystem::file_size(entry.path());
                table_page_counts[table_name] = (file_size + PAGE_SIZE - 1) / PAGE_SIZE;
                if (table_page_counts[table_name] == 0 && file_size > 0) {
                    table_page_counts[table_name] = 1;
                }
            }
        }
    }

    // If sys_tables doesn't have page count, it probably doesn't exist, so nothing to load.
    if (table_page_counts.find("sys_tables") == table_page_counts.end()) {
        return;
    }

    TransactionManager tx_manager(this); // Dummy tx_manager for loading
    auto tables = scan_table("sys_tables", 0, 0, {}, tx_manager);
    for (const auto& table_rec : tables) {
        std::string table_name = table_rec.columns[0].str_value;
        if (metadata.count(table_name)) continue; // Already loaded by directory scan, but let's get schema

        std::vector<Column> cols;
        auto all_cols = scan_table("sys_columns", 0, 0, {}, tx_manager);
        for (const auto& col_rec : all_cols) {
            if (col_rec.columns[0].str_value == table_name) {
                cols.push_back({col_rec.columns[1].str_value, (DataType)col_rec.columns[2].int_value, (bool)col_rec.columns[3].int_value});
            }
        }
        metadata[table_name] = cols;
    }
}

void StorageEngine::create_table(const std::string& table_name, const std::vector<ColumnDefinition>& columns, int tx_id, int cid) {
    if (metadata.count(table_name)) {
        throw std::runtime_error("Table already exists: " + table_name);
    }
    std::string file_path = "data/" + table_name + ".tbl";
    table_files[table_name] = file_path;
    table_page_counts[table_name] = 0;
    
    std::vector<Column> cols;
    for(const auto& col_def : columns) {
        cols.push_back({col_def.name, col_def.type, col_def.not_null});
    }
    metadata[table_name] = cols;

    // Ensure data directory exists
    std::filesystem::create_directories("data");
    
    // Create an empty file for the table
    std::ofstream table_file(file_path);
    if (!table_file) {
        throw std::runtime_error("Could not create table file: " + file_path);
    }
    table_file.close();
    add_new_page_to_table(table_name);

    // Insert into catalog tables
    if (table_name != "sys_tables" && table_name != "sys_columns") {
        Record table_rec; table_rec.columns = {Value(table_name)};
        insert_record("sys_tables", table_rec, tx_id, cid);
        for (const auto& col : cols) {
            Record col_rec; col_rec.columns = {Value(table_name), Value(col.name), Value((int)col.type), Value((int)col.not_null)};
            insert_record("sys_columns", col_rec, tx_id, cid);
        }
    }
    metadata[table_name] = cols;

    write_wal(tx_id, "CREATE_TABLE", table_name);
}

void StorageEngine::create_index(const std::string& table_name, const std::string& column_name) {
    if (!metadata.count(table_name)) {
        throw std::runtime_error("Table not found: " + table_name);
    }
    if (metadata.find(table_name) == metadata.end()) {
        throw std::runtime_error("Table not found in metadata: " + table_name);
    }
    const auto& cols = metadata[table_name];
    auto it = std::find_if(cols.begin(), cols.end(), [&](const Column& col){ return col.name == column_name; });
    if (it == cols.end()) {
        throw std::runtime_error("Column not found: " + column_name);
    }

    std::string index_name = table_name + "_" + column_name + "_idx";
    indexes[index_name] = std::make_unique<BPlusTree>(cache, table_name + "_" + column_name + ".idx");
    
    // Populate the index
    TransactionManager tx_manager(this);
    auto records = scan_table(table_name, 0, 0, {}, tx_manager); // Simplified call
    int col_idx = static_cast<int>(std::distance(cols.begin(), it));

    for(size_t i = 0; i < records.size(); ++i) {
        const auto& rec = records[i];
        if(col_idx < rec.columns.size()) {
            std::string key; 
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
    write_wal(0, "CREATE_INDEX", index_name);
}

void StorageEngine::create_index(const std::string& index_name, const std::string& table_name, const std::string& column_name) {
    if (!metadata.count(table_name)) {
        throw std::runtime_error("Table not found: " + table_name);
    }
    if (metadata.find(table_name) == metadata.end()) {
        throw std::runtime_error("Table not found in metadata: " + table_name);
    }
    const auto& cols = metadata[table_name];
    auto it = std::find_if(cols.begin(), cols.end(), [&](const Column& col){ return col.name == column_name; });
    if (it == cols.end()) {
        throw std::runtime_error("Column not found: " + column_name);
    }

    // Use the user-provided index name
    indexes[index_name] = std::make_unique<BPlusTree>(cache, index_name + ".idx");
    
    // Populate the index
    TransactionManager tx_manager(this);
    auto records = scan_table(table_name, 0, 0, {}, tx_manager); // Simplified call
    int col_idx = static_cast<int>(std::distance(cols.begin(), it));

    for(size_t i = 0; i < records.size(); ++i) {
        const auto& rec = records[i];
        if(col_idx < rec.columns.size()) {
            std::string key; 
			const auto& col_val = rec.columns[col_idx];
            if (col_val.type == DataType::INT) {
                key = std::to_string(col_val.int_value);
            } else if (col_val.type == DataType::STRING) {
                key = col_val.str_value;
            }
            Tid tid = {table_name, static_cast<int>(i / 100), static_cast<short>(i % 100)}; // Create proper Tid
            indexes[index_name]->insert(key, tid);
        }
    }
    write_wal(0, "CREATE_INDEX", index_name);
}

void StorageEngine::insert_record(const std::string& table_name, const Record& record, int tx_id, int cid) {
    char buffer[PAGE_SIZE];
    char* ptr = buffer;

    ptr += sizeof(uint16_t);
    
    *reinterpret_cast<int*>(ptr) = record.xmin; ptr += sizeof(int);
    *reinterpret_cast<int*>(ptr) = record.xmax; ptr += sizeof(int);
    *reinterpret_cast<int*>(ptr) = record.cid; ptr += sizeof(int);
    
    for (const auto& val : record.columns) {
        ptr = serialize_value(ptr, val);
    }
    uint16_t record_size = static_cast<uint16_t>(ptr - buffer);

    *reinterpret_cast<uint16_t*>(buffer) = record_size;

    int page_id = find_page_with_space(table_name, record_size + sizeof(ItemPointer));
    if (table_files.find(table_name) == table_files.end()) {
        throw std::runtime_error("Table not found in file mappings: " + table_name);
    }
    Page* page = cache.get_page(table_files[table_name], page_id);

    page->header.pd_upper -= record_size;
    std::memcpy(page->data + page->header.pd_upper, buffer, record_size);

    // Add item pointer to the array
    if (page->header.item_count < MAX_ITEM_POINTERS) {
        page->item_pointers[page->header.item_count] = {page->header.pd_upper, static_cast<uint16_t>(record_size)};
        page->header.item_count++;
        page->header.pd_lower += sizeof(ItemPointer);
    }
    page->dirty = true;

    update_page_free_space(table_name, page_id, page->header.pd_upper - page->header.pd_lower);
    // Simplified WAL record
    write_wal(tx_id, "INSERT", table_name);
}

std::vector<Record> StorageEngine::scan_table(const std::string& table_name, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager) {
    std::vector<Record> result;
    if (table_page_counts.find(table_name) == table_page_counts.end()) {
        return result;
    }
    if (table_page_counts.find(table_name) == table_page_counts.end()) {
        throw std::runtime_error("Table not found in page counts: " + table_name);
    }
    int page_count = table_page_counts[table_name];
    for (int i = 0; i < page_count; ++i) {
        if (table_files.find(table_name) == table_files.end()) {
            throw std::runtime_error("Table not found in file mappings: " + table_name);
        }
        Page* page = cache.get_page(table_files[table_name], i);
        for (int j = 0; j < page->header.item_count; ++j) {
            const auto& item_ptr = page->item_pointers[j];
            const char* ptr = page->data + item_ptr.offset;
            const char* record_end = ptr + item_ptr.length;
            Record rec;

            ptr += sizeof(uint16_t);
            
            rec.xmin = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            rec.xmax = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            rec.cid = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);

            const auto& cols = get_table_metadata(table_name);
            for (size_t k = 0; k < cols.size() && ptr < record_end; ++k) {
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

void StorageEngine::drop_table(const std::string& table_name) {
    if (table_files.find(table_name) == table_files.end()) {
        throw std::runtime_error("Table not found: " + table_name);
    }
    if (table_files.find(table_name) == table_files.end()) {
        throw std::runtime_error("Table not found in file mappings: " + table_name);
    }
    std::filesystem::remove(table_files[table_name]);
    metadata.erase(table_name);
    table_files.erase(table_name);
    table_page_counts.erase(table_name);
    free_space_maps.erase(table_name);
    write_wal(0, "DROP_TABLE", table_name);
}

void StorageEngine::drop_index(const std::string& index_name) {
    if (indexes.find(index_name) == indexes.end()) {
        throw std::runtime_error("Index not found: " + index_name);
    }
    indexes.erase(index_name);
    write_wal(0, "DROP_INDEX", index_name);
}

int StorageEngine::add_new_page_to_table(const std::string& table_name) {
    if (table_page_counts.find(table_name) == table_page_counts.end()) {
        throw std::runtime_error("Table not found in page counts: " + table_name);
    }
    if (table_files.find(table_name) == table_files.end()) {
        throw std::runtime_error("Table not found in file mappings: " + table_name);
    }
    
    int new_page_id = table_page_counts[table_name]++;
    Page new_page;
    new_page.header.pd_lower = sizeof(PageHeader);
    new_page.header.pd_upper = PAGE_SIZE;
    new_page.dirty = true;
    write_page_to_file(table_files[table_name], new_page, new_page_id);
    update_page_free_space(table_name, new_page_id, new_page.header.pd_upper - new_page.header.pd_lower);
    return new_page_id;
}

int StorageEngine::find_page_with_space(const std::string& table_name, uint16_t required_space) {
    if (free_space_maps.find(table_name) == free_space_maps.end()) {
        free_space_maps[table_name] = {};
    }
    if (free_space_maps.find(table_name) == free_space_maps.end()) {
        free_space_maps[table_name] = {};
    }
    for(auto const& [page_id, free_space] : free_space_maps[table_name]) {
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

const std::vector<Column>& StorageEngine::get_table_metadata(const std::string& table_name) {
    if (metadata.find(table_name) == metadata.end()) {
        load_catalog();
    }
    if (metadata.find(table_name) == metadata.end()) {
        throw std::runtime_error("Table not found in metadata: " + table_name);
    }
    return metadata[table_name];
}

void StorageEngine::write_wal(int tx_id, const std::string& operation, const std::string& data) {
    if (wal_log.is_open()) {
        wal_log << tx_id << " " << operation << " " << data << std::endl;
    }
}

void StorageEngine::flush_buffer_pool() {
    cache.flush_all();
}

// Helper function to evaluate WHERE conditions against a record
bool StorageEngine::evaluate_conditions(const Record& record, const std::vector<WhereCondition>& conditions, const std::vector<Column>& table_metadata) {
    if (conditions.empty()) {
        return true; // No conditions means all records match
    }
    
    for (const auto& condition : conditions) {
        bool condition_met = false;
        
        // Find the column index
        int col_index = -1;
        for (size_t i = 0; i < table_metadata.size(); ++i) {
            if (table_metadata[i].name == condition.column) {
                col_index = i;
                break;
            }
        }
        
        if (col_index == -1 || col_index >= (int)record.columns.size()) {
            return false; // Column not found, record doesn't match
        }
        
        const Value& col_value = record.columns[col_index];
        const Value& filter_value = condition.value;
        
        // Apply the condition
        if (condition.op == "=") {
            condition_met = (col_value.type == filter_value.type) &&
                           ((col_value.type == DataType::INT && col_value.int_value == filter_value.int_value) ||
                            (col_value.type == DataType::STRING && col_value.str_value == filter_value.str_value));
        } else if (condition.op == "!=") {
            condition_met = (col_value.type != filter_value.type) ||
                           ((col_value.type == DataType::INT && col_value.int_value != filter_value.int_value) ||
                            (col_value.type == DataType::STRING && col_value.str_value != filter_value.str_value));
        } else if (condition.op == "<") {
            if (col_value.type == DataType::INT && filter_value.type == DataType::INT) {
                condition_met = col_value.int_value < filter_value.int_value;
            } else if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                condition_met = col_value.str_value < filter_value.str_value;
            }
        } else if (condition.op == ">") {
            if (col_value.type == DataType::INT && filter_value.type == DataType::INT) {
                condition_met = col_value.int_value > filter_value.int_value;
            } else if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                condition_met = col_value.str_value > filter_value.str_value;
            }
        } else if (condition.op == "<=") {
            if (col_value.type == DataType::INT && filter_value.type == DataType::INT) {
                condition_met = col_value.int_value <= filter_value.int_value;
            } else if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                condition_met = col_value.str_value <= filter_value.str_value;
            }
        } else if (condition.op == ">=") {
            if (col_value.type == DataType::INT && filter_value.type == DataType::INT) {
                condition_met = col_value.int_value >= filter_value.int_value;
            } else if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                condition_met = col_value.str_value >= filter_value.str_value;
            }
        } else if (condition.op == "LIKE") {
            if (col_value.type == DataType::STRING && filter_value.type == DataType::STRING) {
                condition_met = col_value.str_value.find(filter_value.str_value) != std::string::npos;
            }
        }
        
        if (!condition_met) {
            return false; // AND logic: if any condition fails, the record doesn't match
        }
    }
    
    return true; // All conditions passed
}

int StorageEngine::delete_records(const std::string& table_name, const std::vector<WhereCondition>& conditions, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager) {
    int deleted_count = 0;
    
    if (table_page_counts.find(table_name) == table_page_counts.end()) {
        return 0;
    }
    
    const auto& table_cols = get_table_metadata(table_name);
    int page_count = table_page_counts[table_name];
    
    for (int i = 0; i < page_count; ++i) {
        Page* page = cache.get_page(table_files[table_name], i);
        bool page_modified = false;
        
        for (int j = 0; j < page->header.item_count; ++j) {
            const auto& item_ptr = page->item_pointers[j];
            const char* ptr = page->data + item_ptr.offset;
            const char* record_end = ptr + item_ptr.length;
            Record rec;
            
            ptr += sizeof(uint16_t);
            rec.xmin = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            rec.xmax = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            rec.cid = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            
            for (size_t k = 0; k < table_cols.size() && ptr < record_end; ++k) {
                Value val;
                ptr = deserialize_value(ptr, val);
                rec.columns.push_back(val);
            }
            
            if (is_visible(rec, tx_id, cid, snapshot, tx_manager) && evaluate_conditions(rec, conditions, table_cols)) {
                // Mark record as deleted by setting xmax
                char* xmax_ptr = page->data + item_ptr.offset + sizeof(uint16_t) + sizeof(int);
                *reinterpret_cast<int*>(xmax_ptr) = tx_id;
                page_modified = true;
                deleted_count++;
            }
        }
        
        if (page_modified) {
            page->dirty = true;
        }
    }
    
    return deleted_count;
}

int StorageEngine::update_records(const std::string& table_name, const std::vector<WhereCondition>& conditions, const std::map<std::string, Value>& set_clause, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager) {
    int updated_count = 0;
    
    if (table_page_counts.find(table_name) == table_page_counts.end()) {
        return 0;
    }
    
    const auto& table_cols = get_table_metadata(table_name);
    int page_count = table_page_counts[table_name]; // Capture original page count to avoid infinite loop
    
    // First pass: collect records to update and mark them as deleted
    std::vector<Record> records_to_update;
    
    for (int i = 0; i < page_count; ++i) {
        Page* page = cache.get_page(table_files[table_name], i);
        bool page_modified = false;
        
        for (int j = 0; j < page->header.item_count; ++j) {
            const auto& item_ptr = page->item_pointers[j];
            const char* ptr = page->data + item_ptr.offset;
            const char* record_end = ptr + item_ptr.length;
            Record rec;
            
            ptr += sizeof(uint16_t);
            rec.xmin = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            rec.xmax = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            rec.cid = *reinterpret_cast<const int*>(ptr); ptr += sizeof(int);
            
            for (size_t k = 0; k < table_cols.size() && ptr < record_end; ++k) {
                Value val;
                ptr = deserialize_value(ptr, val);
                rec.columns.push_back(val);
            }
            
            if (is_visible(rec, tx_id, cid, snapshot, tx_manager) && evaluate_conditions(rec, conditions, table_cols)) {
                // Mark old record as deleted
                char* xmax_ptr = page->data + item_ptr.offset + sizeof(uint16_t) + sizeof(int);
                *reinterpret_cast<int*>(xmax_ptr) = tx_id;
                page_modified = true;
                
                // Store record for updating
                records_to_update.push_back(rec);
            }
        }
        
        if (page_modified) {
            page->dirty = true;
        }
    }
    
    // Second pass: insert updated records
    for (const auto& old_rec : records_to_update) {
        Record updated_rec = old_rec;
        
        // Apply SET clause
        for (const auto& set_pair : set_clause) {
            int col_index = -1;
            for (size_t k = 0; k < table_cols.size(); ++k) {
                if (table_cols[k].name == set_pair.first) {
                    col_index = k;
                    break;
                }
            }
            if (col_index >= 0 && col_index < (int)updated_rec.columns.size()) {
                updated_rec.columns[col_index] = set_pair.second;
            }
        }
        
        // Insert new record version
        updated_rec.xmin = tx_id;
        updated_rec.xmax = 0;
        updated_rec.cid = cid;
        
        insert_record(table_name, updated_rec, tx_id, cid);
        updated_count++;
    }
    
    return updated_count;
}
std::vector<Record> StorageEngine::index_scan(const std::string& table_name, const std::string& column, const Value& value, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager) { return {}; }
void StorageEngine::vacuum_table(const std::string& table_name, TransactionManager& tx_manager) {}
bool StorageEngine::has_index(const std::string& table_name, const std::string& column) const { return false; }