#pragma once

#include <glog/logging.h>

#include <cstddef>
#include <cstdlib>

#include "io/readable_file.h"
#include "storage/disk_storage.h"
#include "storage/memory_storage.h"
#include "storage/pmem_storage.h"
#include "utils/common.h"
#include "utils/schema.h"
#include <thread>
#include <semaphore>
#include <queue>

const std::string WAL_PATH_SUFFIX = std::string("wal.dat");

class StorageEngine {
public:
    StorageEngine(const std::string& aep_dir, const std::string& disk_dir)
            : _storage_path(aep_dir + WAL_PATH_SUFFIX) {
        for (size_t i = 0; i < BUCKET_NUMBER; i++) {
            std::string sub_path = _storage_path + "." + std::to_string(i);

            semaphores.push_back(new std::__atomic_semaphore_base(0));

            thread_pool.push_back(std::thread([&](int id){
                while(1){
                    auto _wal_thread = new PmemStorage(sub_path, _memtable);
                    semaphores[id]->acquire();
                    _wal_thread->write(&row_data[id].front());
                    row_data[id].pop();
                }
            },i));
            row_data.emplace_back();
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
        //_wal[row_ptr->id & BUCKET_NUMBER_MASK]->write(row_ptr);
        row_data[row_ptr->id & BUCKET_NUMBER_MASK].push(*row_ptr);
        semaphores[row_ptr->id & BUCKET_NUMBER_MASK]->release();
    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) {
        return _memtable.read(select_column, where_column, column_key, column_key_len,
                              static_cast<char*>(res));
    }

private:
    std::string _storage_path;
    MemoryStorage _memtable;
    PmemStorage* _wal[BUCKET_NUMBER];
    std::vector<std::thread> thread_pool;
    std::vector<std::queue<const Schema::Row> >row_data;
    std::vector<std::__atomic_semaphore_base * > semaphores;
};
