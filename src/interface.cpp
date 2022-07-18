#include "interface.h"

#include "storage/storage_engine.h"
#include "storage/storage_engine_factory.h"

void engine_write(void* ctx, const void* data, size_t len) {
    auto* engine = static_cast<StorageEngine*>(ctx);
    engine->write(data);
}

size_t engine_read(void* ctx, int32_t select_column, int32_t where_column, const void* column_key,
                   size_t column_key_len, void* res) {
    auto* engine = static_cast<StorageEngine*>(ctx);
    return engine->read(select_column, where_column, column_key, column_key_len, res);
}

void* engine_init(const char* host_info, const char* const* peer_host_info,
                  size_t peer_host_info_num, const char* aep_dir, const char* disk_dir) {
    return StorageEngineFactory::get_storage_engine();
}

void engine_deinit(void* ctx) {
    delete static_cast<StorageEngine*>(ctx);
}
