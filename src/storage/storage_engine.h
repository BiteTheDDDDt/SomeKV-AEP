#pragma once

#include <glog/logging.h>

#include <atomic>
#include <cstddef>
#include <cstdlib>

#include "storage/memory_storage.h"
#include "storage/pmem_storage.h"
#include "utils/common.h"
#include "utils/schema.h"

const std::string WAL_PATH_SUFFIX = std::string("wal.dat");

inline thread_local int wal_id = -1;
inline std::atomic_int wal_number = 0;

class StorageEngine {
public:
    StorageEngine(const std::string& aep_dir, const std::string& disk_dir)
            : _storage_path(aep_dir + WAL_PATH_SUFFIX) {
        for (size_t i = 0; i < BUCKET_NUMBER; i++) {
            std::string sub_path = _storage_path + "." + std::to_string(i);
            _wal[i] = new PmemStorage(sub_path, _memtable);
        }
        LOG(INFO) << "Create StorageEngine. _storage_path=" << _storage_path;
    }

    ~StorageEngine() {
        LOG(INFO) << "Destroy StorageEngine. _storage_path=" << _storage_path;
        for (size_t i = 0; i < BUCKET_NUMBER; i++) {
            delete _wal[i];
        }
    }

    void write(const void* data) {
        const Schema::Row* row_ptr = static_cast<const Schema::Row*>(data);
        _memtable.write(row_ptr);
        if (wal_id == -1) {
            wal_id = wal_number++;
        }
        _wal[wal_id]->write(row_ptr);
    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) const {
        return _memtable.read(select_column, where_column, column_key, column_key_len,
                              static_cast<char*>(res));
    }

private:
    std::string _storage_path;
    MemoryStorage _memtable;
    PmemStorage* _wal[BUCKET_NUMBER];
};
