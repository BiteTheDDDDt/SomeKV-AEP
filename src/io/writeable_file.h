#pragma once

#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include "utils/common.h"
#include "utils/schema.h"

class WriteableFile {
public:
    WriteableFile(std::string path)
            : _fd(open(path.data(), O_APPEND | O_WRONLY | O_CREAT | O_SYNC, 0644)) {}

    ~WriteableFile() {
        flush();
        close(_fd);
    }

    void append(const void* data_ptr) {
        std::memcpy(_buffer + _data_count++ * Schema::ROW_LENGTH, data_ptr, Schema::ROW_LENGTH);

        if (_data_count == WRITE_FILE_BUFFER_SIZE) {
            flush();
        }
    }
    void flush() {
        if (!_data_count) {
            return;
        }

        // LOG(INFO) << "Flush: _data_count=" << _data_count;

        [[maybe_unused]] auto res = write(_fd, _buffer, _data_count * Schema::ROW_LENGTH);
        _data_count = 0;
    }

private:
    int _fd;
    int _data_count = 0;
    char _buffer[WRITE_FILE_BUFFER_SIZE * Schema::ROW_LENGTH];
};
