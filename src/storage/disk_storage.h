#pragma once

#include <mutex>

#include "io/writeable_file.h"

class DiskStorage {
public:
    DiskStorage(const std::string& path) : _wal(path) {}

    void write(const void* row_ptr) {
        std::unique_lock lock(_mtx);
        _wal.append(row_ptr);
    }

private:
    WriteableFile _wal;
    std::mutex _mtx;
};
