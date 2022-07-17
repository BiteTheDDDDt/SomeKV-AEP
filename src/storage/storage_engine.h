#pragma once

#include <cstdint>

struct Schema {
    int64_t id;
    char user_id[128];
    char name[128];
    int64_t salary;
};

class StorageEngine {
public:
    StorageEngine() {}

private:
};