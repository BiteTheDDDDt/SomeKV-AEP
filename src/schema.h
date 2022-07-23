#pragma once

#include <cstdint>

namespace Schema {

struct Row {
    int64_t id;
    char user_id[128];
    char name[128];
    int64_t salary;
};

constexpr int ROW_LENGTH = sizeof(Row);

} // namespace Schema