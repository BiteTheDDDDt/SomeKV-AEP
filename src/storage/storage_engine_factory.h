#pragma once

#include <string>

#include "storage/storage_engine_p2p.h"
#include "storage_engine.h"

struct StorageEngineFactory {
    static StorageEngineP2P* get_storage_engine(const char* aep_dir, const char* disk_dir,
                                                std::string ip, std::vector<std::string> peer_ip) {
        return new StorageEngineP2P(ip, peer_ip, std::string(aep_dir), std::string(disk_dir));
    }
    static StorageEngine* get_storage_engine_local(const char* aep_dir, const char* disk_dir,
                                                   std::string ip,
                                                   std::vector<std::string> peer_ip) {
        return new StorageEngine(std::string(aep_dir), std::string(disk_dir));
    }
};
