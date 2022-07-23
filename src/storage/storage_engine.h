#pragma once

#include <cstdlib>

#include "io/readable_file.h"
#include "schema.h"
#include "storage/memory_storage.h"

class StorageEngine {
public:
    StorageEngine(const std::string& aep_dir) : _memtable(ReadableFile(aep_dir).recover()) {}

    void write(const void* data) {
        const Schema::Row* row_ptr = static_cast<const Schema::Row*>(data);
        _memtable.write(row_ptr);
    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) {
        return _memtable.read(select_column, where_column, column_key, column_key_len, res);
    }

private:
    MemoryStorage _memtable;
};