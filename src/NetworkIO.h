//
// Created by rainbowwing on 2022/9/4.
//

#ifndef SOMEKV_AEP_NETWORKIO_H
#define SOMEKV_AEP_NETWORKIO_H

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include "asio.hpp"
#include <cstdlib>
#include <iostream>
#include <thread>
#include <utility>
#include "asio.hpp"

using asio::ip::tcp;

const int max_length = 8192;

void session(tcp::socket sock, std::function<std::string(char *, int)> call_back) {
    LOG(INFO) << "netio session success get query\n";
    try {
        std::string read_data;
        // read_head
        char data[max_length];
        while (read_data.size() < 10) {

            asio::error_code error;
            size_t length = sock.read_some(asio::buffer(data), error);
            read_data += std::string(data, length);
            if (error == asio::error::eof)
                break; // Connection closed cleanly by peer.
            else if (error)
                throw asio::system_error(error); // Some other error.
        }
        // std::cout << "x: " << read_data << std::endl;
        int x = std::stoi(read_data.substr(2, 8));
        // std::cout << "x: " << x << std::endl;
        while (read_data.size() < 10 + x) {
            char data[max_length];
            asio::error_code error;
            size_t length = sock.read_some(asio::buffer(data), error);
            read_data += std::string(data, length);
            if (error == asio::error::eof)
                break; // Connection closed cleanly by peer.
            else if (error)
                throw asio::system_error(error); // Some other error.
        }
        read_data.erase(0, 10);
        std::string ret_data = call_back(read_data.data(), (int) read_data.length());
        LOG(INFO) << "data  success get \n";

        //puts(read_data.data());

//        asio::write(sock, asio::buffer(read_data.substr(0,8).data(), 8));



        int len_ret = ret_data.length();
        std::string ret_string_length = std::to_string(len_ret);
        while (ret_string_length.size() < 8)
            ret_string_length = "0" + ret_string_length;
        std::string ret = "aa" + ret_string_length + ret_data;
        // std::cout << " ret " << ret_string_length <<" "<<ret_data <<" "<< ret<<  std::endl;

        //memcpy(data,ret.data(),ret.length());
        asio::write(sock, asio::buffer(ret.data(), ret.length()));
        LOG(INFO) << "data  success write back \n";

        /* for (;;)
         {
             char data[max_length];

             asio::error_code error;
             size_t length = sock.read_some(asio::buffer(data), error);
             if (error == asio::error::eof)
                 break; // Connection closed cleanly by peer.
             else if (error)
                 throw asio::system_error(error); // Some other error.
             puts(data);
             memcpy(data,"SUCCESS",sizeof("SUCCESS"));
             asio::write(sock, asio::buffer(data, length));
         }*/

    }
    catch (std::exception &e) {
        std::cerr << "Exception in thread: " << e.what() << "\n";
    }
}


struct NetworkIO {
    std::shared_ptr<asio::io_context> io_context;
    std::shared_ptr<std::thread> th;
    int is_destroy = false;
    //std::function<std::vector<std::string>(char *)>call_back;

private:
public:
    NetworkIO(int port, std::function<std::string(char *, int)> call_back) {
        io_context = std::make_shared<asio::io_context>();
        is_destroy = false;
        th = std::make_shared<std::thread>([&]() {
            tcp::acceptor a(*io_context, tcp::endpoint(tcp::v4(), port));
            for (;!is_destroy;) {
                LOG(INFO) << "server start  thread \n";
                std::thread(session, a.accept(), call_back).detach();
            }
        });

        LOG(INFO) << "netio success build\n";
        //this->call_back = call_back;
    }

    std::string sent(std::string ip, std::string port, char *data, int len) {
        LOG(INFO) << "sent query \n";
        //std::cout << "??? " <<std::endl;
        try {
            tcp::socket s(*io_context);
            int cnt = 0;
            while (1) {
                if (cnt++ > 5) return "";
                try {
                    LOG(INFO) << "connect  st "<< cnt << " "<< ip <<" "<<port <<"\n";
                    asio::connect(s, tcp::resolver(*io_context).resolve(ip.data(), port.data()));
                    LOG(INFO) << "connect  en"<< cnt << "\n";
                }
                catch (std::exception &e) {
                    LOG(INFO) << "err " <<e.what()<<"\n";
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                    continue;
                }
                break;
            }
            LOG(INFO) << "sent success !!!! \n";
            //std::cout << "Enter message: ";
            //        char request[max_length];
            //        std::cin.getline(request, max_length);
            //        size_t request_length = std::strlen(request);
            char *sent_data = new char[len + 10];

            std::string sss = std::to_string(len);
            while (sss.length() < 8) {
                sss = "0" + sss;
            }
            sss = "aa" + sss;
            memcpy(sent_data, sss.data(), 10);
            memcpy(sent_data + 10, data, len);
            asio::write(s, asio::buffer(sent_data, len + 10));
            delete[] sent_data;
            //std::cout << "sent ok " << std::endl;
            char reply[max_length];
            std::string get_ret;
            while (get_ret.size() < 10) {
                size_t reply_length = asio::read(s,
                                                 asio::buffer(reply, 10));
                get_ret += std::string(reply, reply_length);
            }
            int x = std::stoi(get_ret.substr(2, 8));
            while (get_ret.size() < 10 + x) {
                size_t reply_length = asio::read(s, asio::buffer(reply, std::min(x, max_length)));
                get_ret += std::string(reply, reply_length);
            }
            get_ret.erase(0, 10);
            LOG(INFO) << "get sent back success !!!! \n";
            return get_ret;
        } catch (std::exception &e) {
            LOG(INFO) << " sent some err !!!! \n";
            return "";
        }
    }
    ~NetworkIO(){
        LOG(INFO) << "start destroy\n";
        is_destroy = true;
        th->join();
        LOG(INFO) << "end destroy\n";
    }
};


#endif //SOMEKV_AEP_NETWORKIO_H
