#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

#include "io/common.h"
#include "schema.h"
#include "storage/memory_storage.h"

class ReadableFile {
public:
    ReadableFile(std::string path) {
        _fd = open(path.data(), O_RDWR);

        struct ::stat file_stat;
        if (stat(path.data(), &file_stat) != 0) {
            _size = 0;
        } else {
            _size = file_stat.st_size / Schema::ROW_LENGTH;
        }
    }

    MemoryStorage recover() {
        MemoryStorage memtable;

        lseek(_fd, 0, SEEK_SET);

        for (int i = 0; i < _size; i += WRITE_FILE_BUFFER_SIZE) {
            int limit = std::min(WRITE_FILE_BUFFER_SIZE, _size - i);
            [[maybe_unused]] auto res =
                    pread(_fd, _buffer, limit * Schema::ROW_LENGTH, i * Schema::ROW_LENGTH);

            for (int j = 0; j < limit; j++) {
                memtable.write(reinterpret_cast<Schema::Row*>(_buffer + j * Schema::ROW_LENGTH));
            }
        }

        return memtable;
    }

private:
    int _fd;
    uint64_t _size;
    char _buffer[WRITE_FILE_BUFFER_SIZE * Schema::ROW_LENGTH];
};
