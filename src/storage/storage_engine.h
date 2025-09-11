#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <vector>
#include <string>
#include <fstream>
#include <map>
#include <memory>
#include "../buffer/buffer_cache.h"
#include "../index/bplus_tree.h"

const int PAGE_SIZE = 4096;

struct Record {
    int xmin;
    int xmax;
    int cid;
    std::vector<std::string> columns;
};

struct ItemPointer {
    uint16_t offset;
    uint16_t length;
};

struct PageHeader {
    uint16_t pd_lower; // Points to start of free space
    uint16_t pd_upper; // Points to end of free space
};

struct Page {
    PageHeader header;
    std::vector<ItemPointer> item_pointers;
    bool dirty;
    char data[PAGE_SIZE - sizeof(PageHeader) - sizeof(std::vector<ItemPointer>)]; // Simplified
};


class StorageEngine {
public:
    StorageEngine(BufferCache& cache);
    void create_table(const std::string& table_name, const std::vector<std::string>& columns);
    void create_index(const std::string& table_name, const std::string& column);
    void insert_record(const std::string& table_name, const Record& record, int tx_id, int cid);
    std::vector<Record> scan_table(const std::string& table_name, int tx_id, int cid, const std::map<int, int>& snapshot);
    std::vector<Record> index_scan(const std::string& table_name, const std::string& column, const std::string& value, int tx_id, int cid, const std::map<int, int>& snapshot);
    bool has_index(const std::string& table_name, const std::string& column) const;

private:
    BufferCache& cache;
    std::map<std::string, std::vector<std::string>> metadata;
    std::map<std::string, std::vector<std::string>> files;
    std::map<std::string, std::unique_ptr<BPlusTree>> indexes;
    std::ofstream wal_log;

    bool is_visible(const Record& rec, int tx_id, int cid, const std::map<int, int>& snapshot);
    void write_page_to_file(const std::string& file, const Page& page, int page_id);
    void read_page_from_file(const std::string& file, int page_id, Page& page);
    void write_wal(const std::string& operation, const std::string& data);
};

#endif
