#ifndef PAGE_H
#define PAGE_H

#include <vector>
#include <cstdint>

const int PAGE_SIZE = 4096;
const int MAX_ITEM_POINTERS = 100; // Fixed maximum number of item pointers

struct ItemPointer {
    uint16_t offset;
    uint16_t length;
};

struct PageHeader {
    uint16_t pd_lower;     // Points to start of free space
    uint16_t pd_upper;     // Points to end of free space
    uint16_t item_count;   // Number of items on page
    uint16_t special_size; // Size of special space
};

struct Page {
    PageHeader header;
    ItemPointer item_pointers[MAX_ITEM_POINTERS]; // Fixed-size array instead of vector
    bool dirty;
    char data[PAGE_SIZE - sizeof(PageHeader) - sizeof(ItemPointer) * MAX_ITEM_POINTERS - sizeof(bool)];
    
    Page() : header{}, dirty(false) {
        header.pd_lower = sizeof(PageHeader) + sizeof(ItemPointer) * MAX_ITEM_POINTERS;
        header.pd_upper = PAGE_SIZE;
        header.item_count = 0;
        header.special_size = 0;
    }
};

#endif