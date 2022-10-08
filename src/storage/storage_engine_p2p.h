#pragma once

#include <glog/logging.h>

#include <mutex>

#include "storage/storage_engine.h"
#include "utils/common.h"
#include "utils/network_io.h"
#include "utils/schema.h"

class StorageEngineP2P {
public:
    StorageEngineP2P(std::string host, std::vector<std::string> peer_host,
                     const std::string& aep_dir, const std::string& disk_dir)
            : _local(aep_dir, disk_dir) {
        for (auto peer : peer_host) {
            int x = peer.find_first_of(":");
            _peer_host.emplace_back(peer.substr(0, x), peer.substr(x + 1, peer.length() - x - 1));
        }
        int x = host.find_first_of(":");
        _port = host.substr(x + 1, host.length() - x - 1);
        _remote = std::make_shared<NetworkIO>(stoi(_port), _local);

        while (_local.is_empty()) {
            bool all_alive = true;
            for (auto peer : _peer_host) {
                size_t remote_cnt =
                        _remote->read_remote(peer.first, peer.second, nullptr, 0, nullptr);
                if (remote_cnt != 0) {
                    all_alive = false;
                }
            }
            sleep(1);
            if (all_alive) {
                break;
            }
        }

        LOG(INFO) << "Init StorageEngineP2P: " << host << " ,peer_size=" << _peer_host.size();
    }

    ~StorageEngineP2P() {
        LOG(INFO) << "Start destroy StorageEngineP2P";
        _remote->close();
        while (true) {
            bool all_close = true;
            for (auto peer : _peer_host) {
                size_t remote_cnt =
                        _remote->read_remote(peer.first, peer.second, nullptr, 0, nullptr);
                if (remote_cnt ==0) {
                    all_close = false;
                }
            }
            sleep(1);
            if (all_close) {
                break;
            }
        }

        LOG(INFO) << "End destroy StorageEngineP2P";
    }

    void write(const void* data) { _local.write(data); }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, char* res) {
        std::unique_lock<std::mutex> lck(_mtx);
        constexpr int query_length_prefix = sizeof(int32_t) * 2 + sizeof(size_t);
        char buffer[query_length_prefix + column_key_len];

        size_t length =
                encode_query(select_column, where_column, column_key, column_key_len, buffer);

        LOG(INFO) << "send query";
        print_query(select_column, where_column, column_key, column_key_len);

        size_t cnt = 0;

        for (auto peer : _peer_host) {
            size_t remote_cnt = _remote->read_remote(peer.first, peer.second, buffer, length, res);
            LOG(INFO) << "remote_cnt=" << remote_cnt << " ,peer=" << peer.first << ":"
                      << peer.second;
            if (remote_cnt == (size_t)-1) {
                continue;
            }

            cnt += remote_cnt;
            res += remote_cnt * Schema::COLUMN_LENGTH[select_column];
        }
        cnt += _local.read(select_column, where_column, column_key, column_key_len, res);
        LOG(INFO) << "sum_cnt=" << cnt;
        return cnt;
    }

private:
    StorageEngine _local;
    std::string _host;
    std::string _port;
    std::vector<std::pair<std::string, std::string> > _peer_host;
    std::shared_ptr<NetworkIO> _remote;
    std::mutex _mtx;
};
