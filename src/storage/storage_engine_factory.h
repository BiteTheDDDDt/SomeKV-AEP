#pragma once

#include <string>

#include "storage_engine.h"

struct StorageEngineFactory {
    static StorageEngine* get_storage_engine(const char* aep_dir, const char* disk_dir,
                                             std::string ip, std::vector<std::string> peer_ip) {
        return new StorageEngine(ip, peer_ip, std::string(aep_dir), std::string(disk_dir));
    }
};
