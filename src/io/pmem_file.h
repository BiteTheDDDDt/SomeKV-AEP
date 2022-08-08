#pragma once

#include <fcntl.h>
#include <glog/logging.h>
#include <libpmem.h>
#include <unistd.h>

#include "storage/memory_storage.h"
#include "utils/common.h"
#include "utils/schema.h"

constexpr size_t PMEM_HEADER_SIZE = sizeof(size_t);
constexpr size_t PMEM_MAX_ROW_SIZE = (MAX_ROW_SIZE / BUCKET_NUMBER + 1);
constexpr size_t PMEM_FILE_SIZE = Schema::ROW_LENGTH * PMEM_MAX_ROW_SIZE + PMEM_HEADER_SIZE;

class PmemFile {
public:
    PmemFile(const std::string& path, MemoryStorage& memtable) {
        int fd = open(path.data(), O_APPEND | O_WRONLY | O_CREAT, 0644);

        bool need_recover = get_file_size(path) != 0;
        if (!need_recover) {
            int ret = fallocate(fd, 0, 0, PMEM_FILE_SIZE);
            if (ret < 0) {
                printf("ret = %d, errno = %d,  %s\n", ret, errno, strerror(errno));
                LOG(FATAL) << "fallocate fail.";
            }
        }

        close(fd);

        if ((_header = reinterpret_cast<char*>(
                     pmem_map_file(path.data(), 0, 0, 0666, &_mapped_len, &_is_pmem))) == nullptr) {
            perror("pmem_map_file");
            LOG(FATAL) << "pmem_map_file fail.";
        }

        _current = _header + PMEM_HEADER_SIZE;
        if (need_recover) {
            memset(buffer, -1, sizeof(buffer));
            while (_size < PMEM_MAX_ROW_SIZE) {
                if (memcmp(_current, buffer, Schema::ROW_LENGTH) == 0) {
                    break;
                }
                _size++;
                memtable.write_no_lock(_current);
                _current += Schema::ROW_LENGTH;
            }
            LOG(INFO) << "Recover: size=" << _size;
        } else {
            LOG(INFO) << "Init: pre page fault.";
            pmem_memset_persist(_header, -1, PMEM_FILE_SIZE);
            LOG(INFO) << "Init: pre page fault done.";
        }
    }

    ~PmemFile() { pmem_unmap(_header, _mapped_len); }

    void append(const void* data_ptr) {
        pmem_memcpy(_current, data_ptr, Schema::ROW_LENGTH, PMEM_F_MEM_NONTEMPORAL);
        _current += Schema::ROW_LENGTH;
    }

private:
    size_t _mapped_len;
    int _is_pmem;

    char* _current;
    char* _header;

    size_t _size = 0;

    char buffer[Schema::ROW_LENGTH];
};