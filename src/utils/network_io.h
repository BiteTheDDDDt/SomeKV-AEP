#pragma once

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <thread>
#include <utility>

#include "storage/storage_engine.h"
#include "utils/asio.hpp"
#include "utils/common.h"

using asio::ip::tcp;

class NetworkIO {
public:
    NetworkIO(int port, const StorageEngine& local);

    size_t read_remote(std::string ip, std::string port, const char* query, size_t query_length,
                       char* result) {
        asio::io_context io_context;
        tcp::resolver resolver(io_context);
        tcp::socket sock(io_context);

        size_t length = 0;
        try {
            try {
                asio::connect(sock, resolver.resolve(ip.data(), port.data()));
            } catch (std::exception& e) {
                LOG(WARNING) << e.what();
                LOG(INFO) << "Connect fail.";
                return FAIL_FLAG;
            }
            LOG(INFO) << "Connect sucess.";

            if (query_length == 1) {
                sock.write_some(asio::buffer(&_is_close, 1));
                length = sock.read_some(asio::buffer(result, 1));
                return *result;
            } else {
                sock.write_some(asio::buffer(query, query_length));
            }

            length = sock.read_some(asio::buffer(result, MAX_QUERY_BUFFER_LENGTH));
        } catch (std::exception& e) {
            LOG(WARNING) << e.what() << " ,length=" << length;
        }
        if (query_length == 1) {
            return FAIL_FLAG;
        }
        result += length - sizeof(int);
        return *(int*)result;
    }
    ~NetworkIO() {
        _is_destroy = true;

        read_remote("127.0.0.1", std::to_string(_port), nullptr, 0,
                    nullptr); // Send to local a query to avoid blocking.

        _receiver->join();
    }

    static void receive_loop(NetworkIO* io) { io->loop(); }

    static void receive_query(NetworkIO* io, tcp::socket&& sock) { io->receive(std::move(sock)); }

    void loop() {
        while (!_is_destroy) {
            asio::io_context io_context;
            tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), _port));
            LOG(INFO) << "Waiting for accept.";
            std::thread(receive_query, this, acceptor.accept()).detach();
            LOG(INFO) << "Complete a receive.";
        }
    }

    void receive(tcp::socket sock) {
        LOG(INFO) << "receive query from " << sock.remote_endpoint().address() << ":"
                  << sock.remote_endpoint().port() << " to " << sock.local_endpoint().address()
                  << ":" << sock.local_endpoint().port();
        try {
            char buffer[MAX_QUERY_BUFFER_LENGTH];

            // receive query binary
            {
                char* head = buffer;
                size_t length = 0;
                try {
                    length = sock.read_some(asio::buffer(head, MAX_QUERY_BUFFER_LENGTH));
                } catch (std::exception& e) {
                    LOG(WARNING) << e.what();
                }
                LOG(INFO) << "length=" << length;
                if (length == 0 || length == 1) {
                    _received_ip.insert(sock.remote_endpoint().address().to_string());
                    sock.write_some(asio::buffer(buffer, _is_close ? 1 : 0));
                    return;
                }
            }

            // Decode query and read from local, then write result back as [binary_result(x byte) + result_cnt(4 byte)]
            {
                char* head = buffer;
                int32_t select_column;
                int32_t where_column;
                char column_key[128];
                size_t column_key_len;

                decode_query(select_column, where_column, column_key, column_key_len, buffer);

                print_query(select_column, where_column, column_key, column_key_len);

                int cnt =
                        _local.read(select_column, where_column, column_key, column_key_len, head);

                memcpy(head, &cnt, sizeof(int));

                LOG(INFO) << "return_cnt=" << cnt;
                sock.write_some(asio::buffer(
                        buffer, Schema::COLUMN_LENGTH[select_column] * cnt + sizeof(int)));
            }
        } catch (std::exception& e) {
            LOG(WARNING) << e.what();
        }
    }

    void close() { _is_close = true; }

    bool all_received() { return _received_ip.size() == 3; }

private:
    bool _is_close = false;
    bool _is_destroy = false;
    int _port;
    const StorageEngine& _local;
    std::set<std::string> _received_ip;

    std::unique_ptr<std::thread> _receiver;
};

inline NetworkIO::NetworkIO(int port, const StorageEngine& local) : _port(port), _local(local) {
    _receiver = std::make_unique<std::thread>(receive_loop, this);
    LOG(INFO) << "NetworkIO init port=" << port;
}