#include "interface.h"

#include "storage/storage_engine_factory.h"
#include "utils/schema.h"

void engine_write(void* ctx, const void* data, size_t len) {
    if (len != Schema::ROW_LENGTH) {
        LOG(FATAL) << "len!=ROW_LENGTH(len=" << len << ")";
    }
    auto* engine = static_cast<StorageEngineP2P*>(ctx);
    engine->write(data);
}

size_t engine_read(void* ctx, int32_t select_column, int32_t where_column, const void* column_key,
                   size_t column_key_len, void* res) {
    auto* engine = static_cast<StorageEngineP2P*>(ctx);
    return engine->read(select_column, where_column, column_key, column_key_len, (char*)res);
}

void* engine_init(const char* host_info, const char* const* peer_host_info,
                  size_t peer_host_info_num, const char* aep_dir, const char* disk_dir) {
    std::string hostip(host_info);
    std::vector<std::string> peer_ip_port;
    LOG(INFO) << "peer_host_info_num=" << peer_host_info_num;
    for (size_t i = 0; i < peer_host_info_num; i++) {
        peer_ip_port.push_back(std::string(peer_host_info[i]));
        LOG(INFO) << peer_ip_port.back();
    }

    return StorageEngineFactory::get_storage_engine(aep_dir, disk_dir, hostip, peer_ip_port);
}

void engine_deinit(void* ctx) {
    delete static_cast<StorageEngineP2P*>(ctx);
}
