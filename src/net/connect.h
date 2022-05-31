//
// Created by kwins on 2021/5/13.
//

#ifndef CHAKRA_NET_CONNECT_H
#define CHAKRA_NET_CONNECT_H

#include <string>
#include <chrono>
#include <sys/socket.h>
#include "error/err.h"
#include <functional>
#include <array>
#include <atomic>
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
    explicit Connect(net::Connect::Options opts);
    error::Error connect();
    int reconnect();
    error::Error send(const char* data, size_t len);

    void send(Buffer* buffer);
    // 服务端尽可能一次多读，如果读不完，等下次再读，可以有效减少IO次数.
    void receive(Buffer* buffer, const std::function< error::Error (char* ptr, size_t len)>& process);
    /* 客户端使用，一次读取一个包 */
    void receivePack(const std::function<error::Error(char* ptr, size_t len)>& process);

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

    // void moveBuf(int start, int end);
    static void toTimeVal(const milliseconds& duration, timeval& tv);
    static long toMsec(const milliseconds& duration);

    Options opts {};
    int FD;
    sockaddr* sar;
    State state;

    Buffer* cbuffer;  /* client connect use */
    system_clock::time_point lastActive;
};

}



#endif // CHAKRA_NET_CONNECT_H
