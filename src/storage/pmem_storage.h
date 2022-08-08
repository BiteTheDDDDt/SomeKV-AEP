#pragma once

#include <mutex>

#include "io/pmem_file.h"
#include "storage/memory_storage.h"

class PmemStorage {
public:
    PmemStorage(const std::string& path, MemoryStorage& memtable) : _wal(path, memtable) {}

    void write(const void* row_ptr) { _wal.append(row_ptr); }

private:
    PmemFile _wal;
};
