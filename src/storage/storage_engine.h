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
            LOG(INFO) << "host "<<host << "peer host  "<< i << "\n";
            int x = i.find_first_of(":");
            LOG(INFO) << "peerip  "<<i.substr(0,x) << "peer port  "<< i.substr(x+1,i.length()-x) << "\n";
            this->peer_host.emplace_back(i.substr(0,x),
                                         i.substr(x+1,i.length()-x));
        }
        int x = host.find_first_of(":");
        std::string port = host.substr(x+1, host.length()-x);
        LOG(INFO) << "port: " << port << "\n";
        netio = std::make_shared<NetworkIO>(stoi(port),[&](char * data,int x){ //这里得加上个长度！！！
            LOG(INFO) << "do call back\n";
            int32_t select_column = std::stoi(std::string(data,10));
            int32_t where_column = std::stoi(std::string(data+10,10));
            std::string column_key = std::string(data+30,x-30);
            size_t column_key_len = std::stoi(std::string(data+20,10));
            int xx = 4 + 4 + 4 + column_key_len;
            char * r = new char [xx*100];
            int cnt  = read_local(select_column,where_column,column_key.data(),column_key_len,r); // todo 这里要加入把x解析成许多参数
            //返回的是个数 * size
            std::string ret = std::string(r,cnt*xx);
            delete [] r;
            return ret;
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
                size_t column_key_len, void* res) { //TODO RES一定够

        LOG(INFO) << "module get of query read\n";

        char * a = new char[31 + column_key_len];
        std::function<std::string(int32_t)>en_code_int = [&](int32_t x){
            std::string ret = std::to_string(x);
            while(ret.length() !=10)
                ret = "0"+ret;
            return ret;
        };
        std::function<std::string(size_t)>en_code_size = [&](size_t x){
            std::string ret = std::to_string(x);
            while(ret.length() !=10)
                ret = "0"+ret;
            return ret;
        };
       // std::string res = en_code_int(select_column) + en_code_int(where_column) + en_code_size(column_key_len)  + ;
        memcpy(a,en_code_int(select_column).c_str(),10);
        memcpy(a+10,en_code_int(where_column).c_str(),10);
        memcpy(a+20,en_code_int(column_key_len).c_str(),10);
        memcpy(a+30,column_key,column_key_len);
        std::string result;
        LOG(INFO) << "st self query\n";
        result+= netio->sent("127.0.0.1",host,a,30 + column_key_len);
        LOG(INFO) << "en self query\n";

        for(int i=0;i<this->peer_host.size();i++){
            //res +=
           // continue;
            result+= netio->sent(this->peer_host[i].first,this->peer_host[i].second,a,30 + column_key_len); // TODO 这里要把上述参数转化为字符串
        }
        int length = result.length();
        int xx = 4 + 4 + 4 + column_key_len;
        int cnt = length / xx;
      //  char * local_res;
         cnt += _memtable.read(select_column, where_column, column_key, column_key_len,
                       static_cast<char*>(res)+ length); //长度要重新算
        return cnt; // 个数
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
