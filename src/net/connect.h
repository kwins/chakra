//
// Created by kwins on 2021/5/13.
//

#ifndef CHAKRA_NET_CONNECT_H
#define CHAKRA_NET_CONNECT_H

#include <cstddef>
#include <google/protobuf/message.h>
#include <string>
#include <chrono>
#include <sys/socket.h>
#include "error/err.h"
#include <functional>
#include <array>
#include <atomic>
#include <types.pb.h>
#include "buffer.h"

namespace chakra::net {

using namespace std::chrono;

class Connect {
public:
    enum class State {
        INIT,
        CONNECTED,
        CLOSE
    };

    struct Options {
        std::string host;
        int port = 7290;
        milliseconds connectTimeOut { 100 };
        milliseconds readTimeOut { 100 };
        milliseconds writeTimeOut { 100 };
        bool block = false;
        int connectRetrys = 10;
        int fd = -1;
    };

public:
    explicit Connect(net::Connect::Options options);
    error::Error connect();
    int reconnect();

    void writeBuffer(const google::protobuf::Message& message, proto::types::Type type);
    void sendBuffer(); /* send wbuffer data to remote connect */
    void send(Buffer* buffer);
    size_t sendBufferLength();
   
    // 服务端尽可能一次多读，如果读不完，等下次再读，可以有效减少IO次数.
    void receive(Buffer* buffer, const std::function< error::Error (char* ptr, size_t len)>& process);
    void receive(const std::function<error::Error(char* ptr, size_t len)>& process);
    size_t receiveBufferLength();

    // 返回远端地址和端口
    std::string remoteAddr();
    State connState() const;
    long lastActiveTime();  // 毫秒
    void close();
    int fd() const;
    friend void swap(Connect& lc, Connect& rc) noexcept;
    ~Connect();

private:

    error::Error setBlock(bool block);
    error::Error setConnectTimeout();
    error::Error waitConnectReady(long msec);
    error::Error checkConnectOK(int& completed);
    error::Error checkSockErr();

    static void toTimeVal(const milliseconds& duration, timeval& tv);
    static long toMsec(const milliseconds& duration);

    Options opts {};
    int FD;
    sockaddr* sar;
    State state;

    Buffer* wbuffer; /* send data buffer */
    Buffer* rbuffer; /* receive data buffer */
    system_clock::time_point lastActive;
};

}



#endif // CHAKRA_NET_CONNECT_H
