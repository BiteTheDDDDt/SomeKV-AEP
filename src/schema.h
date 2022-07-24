#pragma once

#include <cstdint>
#include <cstring>
#include <string>

namespace Schema {

struct Row {
    int64_t id;
    char user_id[128];
    char name[128];
    int64_t salary;

    std::string to_string() {
        std::string str;

        str += "[";

        str += "id: " + std::to_string(id);
        str += ",";
        str += "user_id: " + std::string(user_id);
        str += ",";
        str += "name: " + std::string(name);
        str += ",";
        str += "salary: " + std::to_string(salary);

        str += "]";

        return str;
    }
};

enum Column { Id = 0, Userid = 1, Name = 2, Salary = 3 };

constexpr int ROW_LENGTH = sizeof(Row);

inline Row create_from_address(const void* address) {
    Row row;
    memcpy(&row, address, ROW_LENGTH);
    return row;
}

inline bool equal(const Row& lhs, const Row& rhs) {
    return memcmp(&lhs, &rhs, ROW_LENGTH) == 0;
}

} // namespace Schema
