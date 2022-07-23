#pragma once

#include <string>

#include "storage_engine.h"

struct StorageEngineFactory {
    static StorageEngine* get_storage_engine(const char* aep_dir, const char* disk_dir) {
        return new StorageEngine(std::string(aep_dir));
    }
};