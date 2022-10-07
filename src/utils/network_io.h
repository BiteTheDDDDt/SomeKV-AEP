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
            asio::connect(sock, resolver.resolve(ip.data(), port.data()));

            if (!query_length) {
                return 0;
            }

            asio::write(sock, asio::buffer(query, query_length));

            length = asio::read(sock, asio::buffer(result, MAX_QUERY_BUFFER_LENGTH));
        } catch (std::exception& e) {
            LOG(WARNING) << e.what() << " length=" << length;
        }

        if (length == 0) {
            return 0;
        }

        result += length - sizeof(int);
        return *(int*)result;
    }
    ~NetworkIO() {
        _is_destroy = true;

        read_remote("127.0.0.1", std::to_string(_port), nullptr, 0,
                    nullptr); // Send to local a query to avoid blocking.
        try {
            _acceptor.cancel();
            _acceptor.close();
        } catch (std::exception& e) {
            LOG(WARNING) << e.what();
        }

        _receiver->join();
    }

    static void receive_loop(NetworkIO* io) { io->loop(); }

    static void receive_query(NetworkIO* io, tcp::socket&& sock) { io->receive(std::move(sock)); }

    void loop() {
        while (!_is_destroy) {
            std::thread(receive_query, this, _acceptor.accept()).detach();
        }
    }

    void receive(tcp::socket sock) {
        LOG(INFO) << "receive query";
        try {
            char buffer[MAX_QUERY_BUFFER_LENGTH];

            // receive query binary
            {
                asio::error_code error;
                char* head = buffer;
                size_t remain_buffer_length = MAX_QUERY_BUFFER_LENGTH;
                while (error != asio::error::eof) {
                    if (remain_buffer_length <= 0) {
                        LOG(WARNING) << "Query length more than buffer size.";
                    }
                    size_t length = sock.read_some(asio::buffer(head, remain_buffer_length), error);
                    LOG(INFO) << "length=" << length;
                    if (!length) {
                        break;
                    }

                    head += length;
                    remain_buffer_length -= length;
                }
            }

            // Decode query and read from local, then write result back as [binary_result(x byte) + result_cnt(4 byte)]
            {
                char* head = buffer;
                int32_t select_column;
                int32_t where_column;
                const void* column_key;
                size_t column_key_len;

                decode_query(select_column, where_column, column_key, column_key_len, buffer);

                print_query(select_column, where_column, column_key, column_key_len);

                int cnt =
                        _local.read(select_column, where_column, column_key, column_key_len, head);

                memcpy(head, &cnt, sizeof(int));

                LOG(INFO) << "cnt=" << cnt << " ,length=" << head - buffer;
                asio::write(sock, asio::buffer(buffer, head - buffer + sizeof(int)));
            }
        } catch (std::exception& e) {
            LOG(WARNING) << e.what();
        }
    }

private:
    bool _is_destroy = false;
    int _port;
    const StorageEngine& _local;
    tcp::acceptor _acceptor;
    std::unique_ptr<std::thread> _receiver;
};

inline NetworkIO::NetworkIO(int port, const StorageEngine& local)
        : _port(port), _local(local), _acceptor({}, tcp::endpoint(tcp::v4(), port)) {
    _receiver = std::make_unique<std::thread>(receive_loop, this);
    LOG(INFO) << "NetworkIO init port=" << port;
}