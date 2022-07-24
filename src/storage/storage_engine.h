#pragma once

#include <glog/logging.h>

#include <cstdlib>
#include <mutex>

#include "io/readable_file.h"
#include "schema.h"
#include "storage/disk_storage.h"
#include "storage/memory_storage.h"

const std::string WAL_PATH_SUFFIX = std::string("/wal.dat");

class StorageEngine {
public:
    StorageEngine(const std::string& aep_dir)
            : _memtable(ReadableFile(aep_dir + WAL_PATH_SUFFIX).recover()),
              _wal(aep_dir + WAL_PATH_SUFFIX) {}

    void write(const void* data) {
        std::unique_lock lock(mtx);
        const Schema::Row* row_ptr = static_cast<const Schema::Row*>(data);
        _memtable.write(row_ptr);
        _wal.write(row_ptr);
    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) {
        std::unique_lock lock(mtx);
        return _memtable.read(select_column, where_column, column_key, column_key_len, res);
    }

private:
    MemoryStorage _memtable;
    DiskStorage _wal;
    std::mutex mtx;
};
