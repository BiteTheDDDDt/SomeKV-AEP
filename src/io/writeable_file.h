#pragma once

#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include "utils/common.h"
#include "utils/schema.h"

class WriteableFile {
public:
    WriteableFile(const std::string& path)
            : _fd(open(path.data(), O_APPEND | O_WRONLY | O_CREAT, 0644)) {}

    ~WriteableFile() { close(_fd); }

    void append(const void* data_ptr) {
        [[maybe_unused]] auto res = write(_fd, data_ptr, Schema::ROW_LENGTH);
    }

private:
    int _fd;
};
