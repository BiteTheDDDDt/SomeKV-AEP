#pragma once

#include <glog/logging.h>
#include <sys/stat.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "utils/schema.h"

constexpr const size_t READ_FILE_BUFFER_SIZE = 1;

constexpr const size_t WRITE_FILE_BUFFER_SIZE = 1;

constexpr size_t BUCKET_NUMBER = 50;

#ifdef MAKE_TEST
constexpr size_t MAX_ROW_SIZE = 500;
#else
constexpr size_t MAX_ROW_SIZE = 50000000;
#endif

constexpr int MAX_QUERY_BUFFER_LENGTH = 4096;

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

inline void print_meminfo() {
    auto cmd = "cat /proc/meminfo";
    FILE* output = popen(cmd, "r");
    char buffer[1024];
    std::string result;
    while (fgets(buffer, 1024, output)) {
        fprintf(stdout, "%s", buffer);
        result += buffer;
    }
    LOG(INFO) << "\n" << result;
    pclose(output);
}

inline size_t encode_query(int32_t select_column, int32_t where_column, const void* column_key,
                           size_t column_key_len, char* buffer) {
    int offset = 0;

    memcpy(buffer + offset, &select_column, sizeof(int32_t));
    offset += sizeof(int32_t);

    memcpy(buffer + offset, &where_column, sizeof(int32_t));
    offset += sizeof(int32_t);

    memcpy(buffer + offset, &column_key_len, sizeof(size_t));
    offset += sizeof(size_t);

    memcpy(buffer + offset, column_key, column_key_len);
    offset += column_key_len;

    return offset;
}

inline void decode_query(int32_t& select_column, int32_t& where_column, const void*& column_key,
                         size_t& column_key_len, const char* buffer) {
    int offset = 0;

    memcpy(&select_column, buffer + offset, sizeof(int32_t));
    offset += sizeof(int32_t);

    memcpy(&where_column, buffer + offset, sizeof(int32_t));
    offset += sizeof(int32_t);

    memcpy(&column_key_len, buffer + offset, sizeof(size_t));
    offset += sizeof(size_t);

    memcpy((char*)column_key, buffer + offset, column_key_len);
}