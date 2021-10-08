//
// Created by kwins on 2021/5/13.
//

#include "connect.h"

#include <cerrno>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>
#include <fcntl.h>
#include "packet.h"
#include <glog/logging.h>

DEFINE_int64(connect_buff_size, 16384, "connect recv max buff size of message, default is 16384");                 /* NOLINT */
// TODO: connect flag difine

chakra::net::Connect::Connect(chakra::net::Connect::Options opts) {
    this->opts = std::move(opts);
    this->sar = nullptr;
    this->errMsg = "";
    this->lastActive = system_clock::now();
    this->buf = (char*) malloc(sizeof(char) * FLAGS_connect_buff_size);
    this->bufSize = FLAGS_connect_buff_size;
    this->bufLen = 0;
    this->bufFree = FLAGS_connect_buff_size;
    if (this->opts.fd > 0){
        this->FD = this->opts.fd;
        this->state = State::CONNECTED;
    } else{
        this->FD = -1;
        this->state = State::INIT;
    }
}

chakra::error::Error chakra::net::Connect::send(const char *data, size_t len) {
    if (data == nullptr || len <= 0) return error::Error("data or len error");
    long size = len;
    long nextLen = size;
    ssize_t sendSize = 0;
    while (true){
        ssize_t writed = ::send(fd(), &data[sendSize], nextLen, MSG_WAITALL);
        if (writed == 0){
            return setError(ERR_SEND,"conect closed");
        } else if (writed < 0){
            if ((errno == EWOULDBLOCK && !opts.block) || errno == EINTR){
                /* try aganin later */
            } else {
                return setError(ERR_SEND,"send");
            }
        } else {
            sendSize += writed;
            if (sendSize == size){
                break;
            } else {
                nextLen = size - sendSize;
            }
        }
    }
    return error::Error();
}

chakra::error::Error chakra::net::Connect::receivePack(const std::function< error::Error (char* ptr, size_t len)>& process) {
    ssize_t readn = ::read(fd(), &buf[bufLen], bufFree);
    if (readn == 0){
        return setError(ERR_RECEIVE, "connect closed");
    } else if (readn < 0){
        if (((errno == EWOULDBLOCK && !opts.block)) || errno == EINTR){
            /* Try again later */
        } else if (errno == ETIMEDOUT && opts.block){
            return setError(ERR_RECEIVE, "read timeout");
        } else {
            return setError(ERR_RECEIVE, "read io");
        }
    } else {
        bufLen += readn;
        bufFree -= readn;
        buf[bufLen] = '\0';
    }
    auto packSize = net::Packet::read<uint64_t>(buf, bufLen, 0);
    if ((packSize == 0) || (packSize > 0 && bufLen < packSize)){
        LOG(WARNING) << "connect recv pack not enough, pack size is " << packSize << " recved is " << bufLen;
        return error::Error(); // 这里不返回错误，只是打印日志，下次继续读取
    }

    // 处理整个包
    // 从缓冲区中删除已经读取的内容
    // 剩下的为未读取, 移动到缓存最前端
    // 如果包解析失败，则丢弃
    auto err = process(buf, packSize);
    moveBuf(packSize, -1);
    return err;
}

std::string chakra::net::Connect::remoteAddr()  {
    if (!opts.host.empty()){
        return opts.host + ":" + std::to_string(opts.port);
    }

    sockaddr_in sa{};
    socklen_t len = sizeof(sa);
    ::getpeername(fd(), (struct sockaddr*)&sa, &len);
    opts.host = inet_ntoa(sa.sin_addr);
    opts.port = ntohs(sa.sin_port);
    return opts.host + ":" + std::to_string(opts.port);
}

chakra::error::Error chakra::net::Connect::connect() {
    if (state == State::CONNECTED) return error::Error();

    int retryTimes = 0;
    int clientFd;
    struct sockaddr_in addr{};

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(opts.host.c_str());
    addr.sin_port = htons(opts.port);

    sar = (struct sockaddr*) malloc(sizeof(struct sockaddr));
    memcpy(sar, (sockaddr*)&addr, sizeof(struct sockaddr));

    if ((clientFd = ::socket(PF_INET, SOCK_STREAM, 0)) < 0){
        return setError("SOCK_STREAM");
    }

    FD = clientFd;

    retry:
    if (::connect(clientFd, (sockaddr*) &addr, sizeof(addr)) < 0){
        if (errno == EHOSTUNREACH){
            return setError("EHOSTUNREACH", true);
        } else if (errno == EINPROGRESS){
            if (!opts.block){
                waitConnectReady(toMsec(opts.connectTimeOut));
            }
        } else if (errno == EADDRNOTAVAIL){
            if (++retryTimes >= opts.connectRetrys){
                return setError("retry");
            } else {
                close();
                goto retry;
            }
        }

        return setError("connect");
    }

    auto err = setBlock(false);
    if (!err.success())
        return err;
    err = setConnectTimeout();
    if (!err.success())
        return err;
    state = State::CONNECTED;
    return err;
}

chakra::error::Error chakra::net::Connect::setBlock(bool block) {
    int flags;
    if ((flags = fcntl(fd(), F_GETFL)) == -1){
        return setError("F_GETFL");
    }

    if (block){
        flags &= ~O_NONBLOCK; // 阻塞
    } else {
        flags &= O_NONBLOCK; // 非阻塞
    }

    if (fcntl(fd(), F_SETFL, flags) == -1){
        return setError(ERR_SET_BLOCK,"F_SETFL", true);
    }

    return error::Error();
}

chakra::error::Error chakra::net::Connect::setError(const std::string& extra, bool closeConn) {
    return setError(errno, extra, closeConn);
}

chakra::error::Error chakra::net::Connect::setError(int code, const std::string &extra, bool closeConn) {
    errMsg.clear();
    if (!extra.empty()){
        errMsg.append(extra);
    }
    if (errno > 0){
        char buf[128] = {0};
        strerror_r(errno, buf, sizeof(buf));
        errMsg.append(":").append(buf);
    }
    if (closeConn) close();
    return error::Error(errMsg);
}

chakra::error::Error chakra::net::Connect::setConnectTimeout() {
    timeval readv{}, writev{};
    toTimeVal(opts.readTimeOut, readv);
    toTimeVal(opts.writeTimeOut, writev);

    if (setsockopt(fd(), SOL_SOCKET, SO_RCVTIMEO, &readv, sizeof(readv))){
        return setError("SO_RCVTIMEO");
    }

    if (setsockopt(fd(), SOL_SOCKET, SO_SNDTIMEO, &writev, sizeof(writev))){
        return setError("SO_SNDTIMEO");
    }
    return error::Error();
}

void chakra::net::Connect::toTimeVal(const std::chrono::milliseconds &duration, timeval &tv) {
    tv.tv_sec = duration.count() / 1000;
    tv.tv_usec = (duration.count() % 1000) * 1000;
}

long chakra::net::Connect::toMsec(const std::chrono::milliseconds &duration) {
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

chakra::error::Error chakra::net::Connect::waitConnectReady(long msec) {
    struct pollfd wfd[1];
    wfd[0].fd = fd();
    wfd[0].events = POLLOUT;

    if (errno != EINPROGRESS){
        return setError("NOT EINPROGRESS");
    }

    int res;
    if ((res = poll(wfd, 1, msec)) == -1){
        return setError("poll", true);
    } else if (res == 0){
        errno = ETIMEDOUT;
        return setError( "ETIMEDOUT", true);
    }

    if (!checkConnectOK(res).success() || res == 0){
        return checkSockErr();
    }

    return error::Error();
}

chakra::error::Error chakra::net::Connect::checkSockErr() {
    int err1 = 0;
    int err_saved = errno;
    socklen_t err1len = sizeof(err1);
    if (getsockopt(fd(), SOL_SOCKET, SO_ERROR, &err1, &err1len) == -1){
        return setError("SO_ERROR");
    }

    if (err1 == 0) err1 = err_saved;

    if (err1){
        return setError("CHECK SOCK");
    }
    return error::Error();
}

chakra::error::Error chakra::net::Connect::checkConnectOK(int &completed) {
    int rc = ::connect(fd(), const_cast<const struct sockaddr*>(sar), sizeof(sar));
    if (rc == 0){
        completed = 1;
        return error::Error();
    }

    switch (errno) {
        case EISCONN:
            completed = 1;
            break;
        case EALREADY:
        case EINPROGRESS:
        case EWOULDBLOCK:
            completed = 0;
            return error::Error();
        default:
            return error::Error("check connect");
    }
    return error::Error();
}

int chakra::net::Connect::fd() const { return FD; }

chakra::net::Connect::State chakra::net::Connect::connState() const { return state; }

long chakra::net::Connect::lastActiveTime() { return lastActive.time_since_epoch().count(); }

int chakra::net::Connect::reconnect() {
    Connect conn(opts);
    if (!conn.connect().success()){
        return -1;
    }
    swap(*this, conn);
    return 0;
}

void chakra::net::Connect::close() {
    if (FD == -1) return;
    ::close(FD);
    FD = -1;
    state = State::CLOSE;
}

chakra::net::Connect::~Connect() {
    close();
    if (sar != nullptr) free(sar);
    if (buf != nullptr) free(buf);
}

void chakra::net::swap(chakra::net::Connect &lc, chakra::net::Connect &rc) noexcept {
    std::swap(lc.opts, rc.opts);
    std::swap(lc.FD, rc.FD);
    std::swap(lc.sar, rc.sar);
    std::swap(lc.state, rc.state);
    std::swap(lc.errMsg, rc.errMsg);
    std::swap(lc.buf, rc.buf);
    std::swap(lc.bufFree, rc.bufFree);
    std::swap(lc.bufSize, rc.bufSize);
    std::swap(lc.lastActive, rc.lastActive);
}

void chakra::net::Connect::moveBuf(int start, int end) {
    int len = bufLen;
    int newLen = 0;
    if (len == 0) return;
    if (start < 0) {
        start = len+start;
        if (start < 0) start = 0;
    }
    if (end < 0) {
        end = len+end;
        if (end < 0) end = 0;
    }
    newLen = (start > end) ? 0 : (end-start)+1;
    if (newLen != 0) {
        if (start >= (signed)len) {
            newLen = 0;
        } else if (end >= (signed)len) {
            end = len-1;
            newLen = (start > end) ? 0 : (end-start)+1;
        }
    } else {
        start = 0;
    }

    // 如果有需要，对字符串进行移动
    // T = O(N)
    if (start && newLen) memmove(buf, buf+start, newLen);
    buf[newLen] = '\0';
    bufFree = bufFree + (len - newLen);
    bufLen = newLen;
}
