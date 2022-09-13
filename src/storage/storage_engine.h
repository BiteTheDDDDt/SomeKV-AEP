#pragma once

#include <glog/logging.h>

#include <atomic>
#include <cstddef>
#include <cstdlib>

#include "io/readable_file.h"
#include "storage/disk_storage.h"
#include "storage/memory_storage.h"
#include "storage/pmem_storage.h"
#include "NetworkIO.h"
#include "utils/common.h"
#include "utils/schema.h"

const std::string WAL_PATH_SUFFIX = std::string("wal.dat");

inline thread_local int wal_id = -1;
inline std::atomic_int wal_number = 0;

class StorageEngine {
public:
    StorageEngine(std::string host,std::vector<std::string>peer_host,const std::string& aep_dir, const std::string& disk_dir)
            : _storage_path(aep_dir + WAL_PATH_SUFFIX) {
        for (size_t i = 0; i < BUCKET_NUMBER; i++) {
            std::string sub_path = _storage_path + "." + std::to_string(i);
            _wal[i] = new PmemStorage(sub_path, _memtable);
        }
        for(auto i : peer_host){
            int x = i.find_first_of(":");
            this->peer_host.emplace_back(i.substr(0,x),
                                         i.substr(x+1,i.length()));
        }
        std::string port = host.substr(0, host.find_first_of(":"));
        netio = std::make_shared<NetworkIO>(stoi(port),[](char * x){
            read_local(); // todo 这里要加入把x解析成许多参数
        });

        LOG(INFO) << "Create StorageEngine. _storage_path=" << _storage_path;
    }

    ~StorageEngine() {
        LOG(INFO) << "Destroy StorageEngine. _storage_path=" << _storage_path;
        for (size_t i = 0; i < BUCKET_NUMBER; i++) {
            delete _wal[i];
        }
    }



    void write(const void* data) {
        std::unique_lock<std::mutex>lock(mu);
        const Schema::Row* row_ptr = static_cast<const Schema::Row*>(data);
        _memtable.write(row_ptr);
        if (wal_id == -1) {
            wal_id = wal_number++;
        }

        _wal[wal_id]->write(row_ptr);

        /*for(auto i : peer_host){
            netio->sent(i.first,i.second,static_cast<char *> data,272);
        }*/

    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) {



        string res;
        for(int i=0;i<this->peer_host.size();i++){
            res += netio->sent(this->peer_host[i].first,this->peer_host[i].second,,); // TODO 这里要把上述参数转化为字符串
        }


        return _memtable.read(select_column, where_column, column_key, column_key_len,
                              static_cast<char*>(res));
    }

    size_t read_local(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) {


        return _memtable.read(select_column, where_column, column_key, column_key_len,
                              static_cast<char*>(res));
    }


private:
    std::string _storage_path;
    MemoryStorage _memtable;
    PmemStorage* _wal[BUCKET_NUMBER];
    std::string host;
    std::vector<std::pair<std::string,std::string> > peer_host;
    std::shared_ptr<NetworkIO> netio;
    std::mutex mu;

};
