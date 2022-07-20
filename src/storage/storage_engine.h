#pragma once

#include <cstdlib>
#include <cstring>
#include <vector>

#include "memtable.h"
#include "schema.h"

class StorageEngine {
public:
    StorageEngine() {}

    void write(const void* data) {
        const Schema::Row* row_ptr = static_cast<const Schema::Row*>(data);
        memtable.write(row_ptr);
    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) {
        return memtable.read(select_column, where_column, column_key, column_key_len, res);
    }

private:
    Memtable memtable;
};