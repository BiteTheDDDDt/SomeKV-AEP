#pragma once

#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include "storage/memory_storage.h"
#include "utils/common.h"
#include "utils/schema.h"

class ReadableFile {
public:
    ReadableFile(const std::string& path)
            : _fd(open(path.data(), O_RDONLY)), _size(get_file_size(path) / Schema::ROW_LENGTH) {
        LOG(INFO) << "Recover: _size=" << _size << ", get_file_size=" << get_file_size(path);
        if (_size * Schema::ROW_LENGTH != get_file_size(path)) {
            LOG(WARNING) << "Recover: file size not match, _size*Schema::ROW_LENGTH="
                         << _size * Schema::ROW_LENGTH;
        }
    }

    ~ReadableFile() { close(_fd); }

    void recover(MemoryStorage& memtable) {
        lseek(_fd, 0, SEEK_SET);

        for (size_t i = 0; i < _size; i += READ_FILE_BUFFER_SIZE) {
            int limit = std::min(READ_FILE_BUFFER_SIZE, _size - i);
            [[maybe_unused]] auto res =
                    pread(_fd, _buffer, limit * Schema::ROW_LENGTH, i * Schema::ROW_LENGTH);

            for (int j = 0; j < limit; j++) {
                memtable.write(create_from_address_ref(_buffer + j * Schema::ROW_LENGTH));
            }
        }
    }

private:
    int _fd;
    uint64_t _size;
    char _buffer[READ_FILE_BUFFER_SIZE * Schema::ROW_LENGTH];
};
