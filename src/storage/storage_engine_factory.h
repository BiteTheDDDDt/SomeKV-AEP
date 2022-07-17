#pragma once

#include "storage_engine.h"

struct StorageEngineFactory {
    static StorageEngine* get_storage_engine() { return new StorageEngine(); }
};