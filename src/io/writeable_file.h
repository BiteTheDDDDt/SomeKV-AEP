#pragma once

#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include "utils/common.h"
#include "utils/schema.h"

class WriteableFile {
public:
    WriteableFile(const std::string& path) : _fp(fopen(path.data(), "ab")) {}

    ~WriteableFile() { fclose(_fp); }

    void append(const void* data_ptr) {
        [[maybe_unused]] auto res = fwrite(data_ptr, 1, Schema::ROW_LENGTH, _fp);
        sync();
    }
    void sync() { [[maybe_unused]] auto res = fflush(_fp); }

private:
    FILE* _fp;
};
