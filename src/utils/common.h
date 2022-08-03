#pragma once

#include <sys/stat.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "utils/schema.h"

constexpr const size_t READ_FILE_BUFFER_SIZE = 1;

constexpr const size_t WRITE_FILE_BUFFER_SIZE = 1;

constexpr size_t BUCKET_NUMBER = (1 << 6);
constexpr size_t BUCKET_NUMBER_MASK = BUCKET_NUMBER - 1;

inline uint64_t get_file_size(std::string path) {
    struct ::stat file_stat;
    if (stat(path.data(), &file_stat) != 0) {
        return 0;
    } else {
        return file_stat.st_size;
    }
}

inline std::string vector_to_string(const std::vector<size_t> v) {
    std::string str;

    str += '[';
    for (auto x : v) {
        if (str.length() > 1) {
            str += ",";
        }
        str += std::to_string(x);
    }
    str += ']';

    return str;
}

inline Schema::Row create_from_address(const void* address) {
    Schema::Row row;
    memcpy(&row, address, Schema::ROW_LENGTH);
    return row;
}

inline int64_t create_from_int64(const void* address) {
    return *static_cast<const int64_t*>(address);
}

inline std::string create_from_string128(const void* address) {
    std::string str;
    str.resize(128);
    memcpy(str.data(), address, 128);
    return str;
}

inline const Schema::Row* create_from_address_ref(const void* address) {
    return static_cast<const Schema::Row*>(address);
}

inline std::string_view create_from_string128_ref(const void* address) {
    return std::string_view(static_cast<const char*>(address), 128);
}

inline bool equal(const Schema::Row& lhs, const Schema::Row& rhs) {
    return memcmp(&lhs, &rhs, Schema::ROW_LENGTH) == 0;
}
