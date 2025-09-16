#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <vector>
#include <string>
#include <fstream>
#include <map>
#include <memory>
#include "../index/bplus_tree.h"
#include "../parser/sql_parser.h"
#include "../common/value.h"
#include "../common/page.h"

// Forward declarations to avoid circular dependency
class TransactionManager;
class BufferCache;

struct Record {
    int xmin;
    int xmax;
    int cid;
    std::vector<Value> columns;
};

struct Column {
    std::string name;
    DataType type;
    bool not_null;
};


class StorageEngine {
public:
    StorageEngine(BufferCache& cache);
    void create_table(const std::string& table_name, const std::vector<ColumnDefinition>& columns, int tx_id, int cid);
    void create_index(const std::string& table_name, const std::string& column);
    void create_index(const std::string& index_name, const std::string& table_name, const std::string& column);
    void insert_record(const std::string& table_name, const Record& record, int tx_id, int cid);
    std::vector<Record> scan_table(const std::string& table_name, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager);
    std::vector<Record> index_scan(const std::string& table_name, const std::string& column, const Value& value, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager);
    int delete_records(const std::string& table_name, const std::vector<WhereCondition>& conditions, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager);
    int update_records(const std::string& table_name, const std::vector<WhereCondition>& conditions, const std::map<std::string, Value>& set_clause, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager);
    bool has_index(const std::string& table_name, const std::string& column) const;
    void write_page_to_file(const std::string& file, const Page& page, int page_id);
    void read_page_from_file(const std::string& file, int page_id, Page& page);
    void drop_table(const std::string& table_name);
    void drop_index(const std::string& index_name);
    void vacuum_table(const std::string& table_name, TransactionManager& tx_manager);
    const std::vector<Column>& get_table_metadata(const std::string& table_name);
    void recover_from_wal();
    void write_wal(int tx_id, const std::string& operation, const std::string& data);
    void flush_buffer_pool();

private:
    BufferCache& cache;
    std::map<std::string, std::vector<Column>> metadata;
    std::map<std::string, std::string> table_files; // table_name -> file_path
    std::map<std::string, int> table_page_counts;
    std::map<std::string, std::map<int, uint16_t>> free_space_maps; // table_name -> {page_id -> free_space}
    std::map<std::string, std::unique_ptr<BPlusTree>> indexes;
    std::fstream wal_log;

    void bootstrap_catalog();
    void load_catalog();

    int add_new_page_to_table(const std::string& table_name);
    int find_page_with_space(const std::string& table_name, uint16_t required_space);
    void update_page_free_space(const std::string& table_name, int page_id, uint16_t new_free_space);

    bool is_visible(const Record& rec, int tx_id, int cid, const std::map<int, int>& snapshot, TransactionManager& tx_manager);
    bool evaluate_conditions(const Record& record, const std::vector<WhereCondition>& conditions, const std::vector<Column>& table_metadata);
};

#endif
