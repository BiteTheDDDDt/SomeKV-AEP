#pragma once

#include <sys/stat.h>

#include <cstddef>
#include <cstdint>
#include <string>

constexpr const size_t READ_FILE_BUFFER_SIZE = 512;

constexpr const size_t WRITE_FILE_BUFFER_SIZE = 1;

inline uint64_t get_file_size(std::string path) {
    struct ::stat file_stat;
    if (stat(path.data(), &file_stat) != 0) {
        return 0;
    } else {
        return file_stat.st_size;
    }
}
