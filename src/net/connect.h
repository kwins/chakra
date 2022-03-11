//
// Created by kwins on 2021/5/13.
//

#ifndef LIBGOSSIP_CONNECT_H
#define LIBGOSSIP_CONNECT_H

#include <string>
#include <chrono>
#include <sys/socket.h>
#include "error/err.h"
#include <functional>
#include <array>
#include <atomic>

namespace chakra::net{

using namespace std::chrono;

class Connect {
public:
    enum class State{
        INIT,
        CONNECTED,
        CLOSE
    };

    struct Options{
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
    explicit Connect(chakra::net::Connect::Options opts);
    error::Error connect();
    int reconnect();
    chakra::error::Error send(const char* data, size_t len);
    // 服务端尽可能一次读完，如果读不完，等下次再度，可以有效减少IO次数.
    // 处理数据的 回调函数 一次只会处理一个 pack
    chakra::error::Error receivePack(const std::function<error::Error(char* ptr, size_t len)>& process);
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

    void moveBuf(int start, int end);
    static void toTimeVal(const milliseconds& duration, timeval& tv);
    static long toMsec(const milliseconds& duration);

    Options opts {};
    int FD;
    sockaddr* sar;
    State state;

    char* buf;
    size_t bufFree; // 可用长度
    size_t bufLen;  // 已缓存长度
    size_t bufSize; // 缓存大小
    system_clock::time_point lastActive;
};

}



#endif //LIBGOSSIP_CONNECT_H
