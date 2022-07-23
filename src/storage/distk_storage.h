#pragma once

#include "io/writable_file.h"

class DiskStorage {
public:
    DiskStorage(std::string path) : _wal(path) {}

    void write(const Schema::Row* row_ptr) { _wal.append(row_ptr); }

private:
    WritableFile _wal;
};