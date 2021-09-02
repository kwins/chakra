//
// Created by kwins on 2021/5/13.
//

#ifndef LIBGOSSIP_CONNECT_H
#define LIBGOSSIP_CONNECT_H

#include <string>
#include <chrono>
#include <sys/socket.h>
#include "utils/error.h"
#include <functional>

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
        int fd = -1;
        std::string host = "";
        int port = 7290;
        int bufSize = (1024*16);
        milliseconds connectTimeOut { 100 };
        milliseconds readTimeOut { 100 };
        milliseconds writeTimeOut { 100 };
        bool block = false;
        int connectRetrys = 10;
    };

public:
    explicit Connect(chakra::net::Connect::Options opts);
    utils::Error connect();
    int reconnect();
    chakra::utils::Error send(const char* data, size_t len);
    // 服务端尽可能一次读完，如果读不完，等下次再度，可以有效减少IO次数.
    // 处理数据的 回调函数 一次只会处理一个 pack
    chakra::utils::Error receivePack(const std::function<void(char* ptr, size_t len)>& process);
    // 返回远端地址和端口
    std::string remoteAddr();
    State connState() const;
    long lastActiveTime();  // 毫秒
    void close();
    int fd() const;
    friend void swap(Connect& lc, Connect& rc) noexcept;
    ~Connect();

private:

    utils::Error setBlock(bool block);
    utils::Error setError(const std::string& extra = "", bool closeConn = false);
    utils::Error setError(int errNo, const std::string& extra = "", bool closeConn = false);

    utils::Error setConnectTimeout();
    utils::Error waitConnectReady(long msec);
    utils::Error checkConnectOK(int& completed);
    utils::Error checkSockErr();

    void bufRange(int start, int end);
    static void toTimeVal(const milliseconds& duration, timeval& tv);
    static long toMsec(const milliseconds& duration);

    static const int ERR_TIMEOUT = 1;
    static const int ERR_CONNECT = 2;
    static const int ERR_SET_BLOCK = 3;
    static const int ERR_RECEIVE = 4;
    static const int ERR_SEND = 5;
    static const int ERR_BAD_ARGS = 5;

    template<typename T>
    T readBuf(int idx = 0){
        size_t len = sizeof(T);
        if (bufLen < len) return 0;
        char cr[len];
        for (int i = idx; i < len; ++i) {
            cr[i] = buf[i];
        }
        return *((T*)cr);
    }

    Options opts {};
    int FD;
    sockaddr* sar;
    State state;
    std::string errMsg;

    char* buf;
    size_t bufFree; // 可用长度
    size_t bufLen; // 已缓存长度
    size_t bufSize; // 缓存大小

    system_clock::time_point lastActive;
};

}



#endif //LIBGOSSIP_CONNECT_H
