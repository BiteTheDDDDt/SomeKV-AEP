#pragma once

#include <fcntl.h>
#include <glog/logging.h>
#include <libpmem2.h>
#include <libpmem2/base.h>
#include <unistd.h>

#include "storage/memory_storage.h"
#include "utils/common.h"
#include "utils/schema.h"

constexpr size_t PMEM_HEADER_SIZE = sizeof(size_t);

constexpr size_t PMEM_FILE_SIZE =
        Schema::ROW_LENGTH * (MAX_ROW_SIZE / BUCKET_NUMBER + 1) + PMEM_HEADER_SIZE;

class PmemFile {
public:
    PmemFile(const std::string& path, MemoryStorage& memtable) {
        _fd = open(path.data(), O_APPEND | O_WRONLY | O_CREAT, 0644);

        bool need_recover = get_file_size(path) != 0;
        if (!need_recover) {
            int ret = fallocate(_fd, 0, 0, PMEM_FILE_SIZE);
            if (ret < 0) {
                printf("ret = %d, errno = %d,  %s\n", ret, errno, strerror(errno));
                LOG(FATAL) << "fallocate fail.";
            }
        }

        close(_fd);

        _fd = open(path.data(), O_RDWR, 0644);

        if (pmem2_config_new(&_cfg)) {
            pmem2_perror("pmem2_config_new fail.");
        }

        if (pmem2_source_from_fd(&_src, _fd)) {
            pmem2_perror("pmem2_source_from_fd fail.");
        }

        if (pmem2_config_set_required_store_granularity(_cfg, PMEM2_GRANULARITY_PAGE)) {
            pmem2_perror("pmem2_config_set_required_store_granularity fail.");
        }

        if (pmem2_map_new(&_map, _cfg, _src)) {
            pmem2_perror("pmem2_map_new fail.");
        }

        _header = reinterpret_cast<char*>(pmem2_map_get_address(_map));
        _current = _header + PMEM_HEADER_SIZE;

        _memcpy = pmem2_get_memcpy_fn(_map);

        if (need_recover) {
            _size = *reinterpret_cast<size_t*>(_header);
            for (size_t i = 0; i < _size; i++) {
                memtable.write_no_lock(_current);
                _current += Schema::ROW_LENGTH;
            }
        }
    }

    ~PmemFile() {
        pmem2_map_delete(&_map);
        pmem2_source_delete(&_src);
        pmem2_config_delete(&_cfg);
        close(_fd);
    }

    void append(const void* data_ptr) {
        _memcpy(_current, data_ptr, Schema::ROW_LENGTH, 0);
        _current += Schema::ROW_LENGTH;

        _size++;
        _memcpy(_header, &_size, PMEM_HEADER_SIZE, 0);
    }

private:
    int _fd;
    struct pmem2_config* _cfg;
    struct pmem2_map* _map;
    struct pmem2_source* _src;
    pmem2_memcpy_fn _memcpy;

    char* _current;
    char* _header;

    size_t _size = 0;
};
