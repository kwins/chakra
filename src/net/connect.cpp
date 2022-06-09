//
// Created by kwins on 2021/5/13.
//

#include "connect.h"

#include <cerrno>
#include <arpa/inet.h>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <error/err.h>
#include <net/buffer.h>
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include "packet.h"

DEFINE_int64(connect_buff_size, 16384, "connect recv max buff size of message, default is 16384");  /* NOLINT */
static bool validConnectBufferSize(const char* flagname, int64_t value){
    if (value <= 0) return false;
    LOG(INFO) << "[connect] new buffer size is " << value;
    return true;
}
DEFINE_validator(connect_buff_size, validConnectBufferSize);

chakra::net::Connect::Connect(chakra::net::Connect::Options options) {
    opts = std::move(options);
    sar = nullptr;
    lastActive = system_clock::now();
    wbuffer = new chakra::net::Buffer(FLAGS_connect_buff_size);
    rbuffer = new chakra::net::Buffer(FLAGS_connect_buff_size);
    if (opts.fd > 0) {
        FD = opts.fd;
        state = State::CONNECTED;
    } else {
        FD = -1;
        state = State::INIT;
    }
}
void chakra::net::Connect::writeBuffer(const google::protobuf::Message& message, proto::types::Type type) {  wbuffer->writeMsg(message, type); }
void chakra::net::Connect::sendBuffer() { send(wbuffer); }
void chakra::net::Connect::send(Buffer* buffer) {
    if (buffer->data == nullptr) return;
    if (buffer->len == 0) return;
    size_t size = buffer->len;
    size_t nextlen = size;
    ssize_t sended = 0;
    while (true) {
        // linux下当服务器连接断开，客户端还发数据的时候，因为连接失败发送出错，
        // 不仅send()的返回值会有反映，而且还会像系统发送一个异常消息，如果不作处理，系统会出 BrokePipe，程序会退出。
        ssize_t writed = ::write(fd(), &buffer->data[sended], nextlen);
        if (writed == 0) {
            throw error::ConnectClosedError("connect closed");;
        } else if (writed < 0) {
            if ((errno == EWOULDBLOCK && !opts.block) || errno == EINTR) {
                /* try aganin later */
            } else {
                throw error::Error(strerror(errno));
            }
        } else {
            sended += writed;
            if (sended == size) { /* send finished */
                buffer->move(sended, -1);
                break;
            } else { /* continue send */
                nextlen = size - sended;
            }
        }
    }
}

void chakra::net::Connect::receive(const std::function<error::Error(char* ptr, size_t len)>& process) {  receive(rbuffer, process); }
void chakra::net::Connect::receive(Buffer* buffer, const std::function<error::Error(char* ptr, size_t len)>& process) {
    // DLOG(INFO) << "####[connect:" << remoteAddr() << "] start receive data while buffer len " << buffer->len << " free " << buffer->free << " size " << buffer->size;
    do { /* read as many messages as possible */
        buffer->maybeRealloc();
        ssize_t readn = ::read(fd(), &buffer->data[buffer->len], buffer->bfree);
        if (readn == 0) {
            throw error::ConnectClosedError("connect closed");
        } else if (readn < 0) {
            // EWOULDBLOCK 表示发送时套接字发送缓冲区已满，或接收时套接字接收缓冲区为空
            if (((errno == EWOULDBLOCK && !opts.block)) || errno == EINTR) {
                /* Try again later */
                DLOG(INFO) << "[connect] try read again with errno " << errno;
            } else if (errno == ETIMEDOUT && opts.block) {
                throw error::ConnectReadError(strerror(errno));
            } else {
                throw error::ConnectReadError(strerror(errno));
            }
        } else {
            buffer->len += readn;
            buffer->bfree -= readn;
            buffer->data[buffer->len] = '\0';
        }

        auto packSize = buffer->read<uint64_t>(0);
        if (packSize > 0 && buffer->len >= packSize) break;
        LOG_IF(INFO, readn > 0) << "[connect] recv pack not enough, pack size is " << packSize  
                     << " buffer len is " << buffer->len 
                     << " buffer free is " << buffer->bfree
                     << " read size " << readn;
    } while (true);

    int processed = 0;
    do { /* process as many messages as possible */
        auto packSize = buffer->read<uint64_t>(0);
        if ((packSize == 0) || (packSize > 0 && buffer->len < packSize)) { /* it means that no data or not enough for a complete package in buffer */
            LOG(WARNING) << "[connect] recv pack not enough, pack size is " << packSize << " buffer len is " << buffer->len;
            return;
        }

        // process a package
        // delete readed data from buffer
        // move to head if remain some data in buffer
        // drop it if process fail
        // TODO: 在这里根据 type 构造不同的 protobuf 对象，传入 process
        auto err = process(buffer->data, packSize);
        // DLOG(INFO) << "[connect:" << remoteAddr() << "] package has been processed and it's size is " << packSize 
        //            << " and read size is " << readn << " and buffer len " << buffer->len << " free " << buffer->free << " size " << buffer->size;
        buffer->move(packSize, -1);
        // DLOG(INFO) << "[connect:" << remoteAddr() << "] package has been moved and buffer len " << buffer->len << " free " << buffer->free << " size " << buffer->size;
        processed++;
        if (err) throw err;
    } while (buffer->len > 0);
    // DLOG(INFO) << "[connect:" << remoteAddr() << "] buffer process finish(" << processed << ")" << "\n\n\n";
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

    if ((clientFd = ::socket(PF_INET, SOCK_STREAM, 0)) < 0) {
        return error::Error(strerror(errno));
    }
    FD = clientFd;

    retry:
    if (::connect(clientFd, (sockaddr*) &addr, sizeof(addr)) < 0) {
        if (errno == EHOSTUNREACH) {
            close();
            return error::Error(strerror(errno));
        } else if (errno == EINPROGRESS) {
            if (!opts.block) { // wait for connect timeout
                auto err = waitConnectReady(toMsec(opts.connectTimeOut));
                if (err) return err;
                goto next;
            }
        } else if (errno == EADDRNOTAVAIL) {
            if (++retryTimes >= opts.connectRetrys) {
                return error::Error("connect retry limit");
            } else {
                close();
                goto retry;
            }
        }
        return error::Error(strerror(errno));
    }
    
    next:
    auto err = setBlock(false);
    if (err)
        return err;
    err = setConnectTimeout();
    if (err)
        return err;
    state = State::CONNECTED;
    return err;
}

chakra::error::Error chakra::net::Connect::setBlock(bool block) {
    int flags;
    if ((flags = fcntl(fd(), F_GETFL)) == -1){
        return error::Error(strerror(errno));
    }

    if (block){
        flags &= ~O_NONBLOCK; // 阻塞
    } else {
        flags &= O_NONBLOCK; // 非阻塞
    }

    if (fcntl(fd(), F_SETFL, flags) == -1) {
        close();
        return error::Error(strerror(errno));
    }

    return error::Error();
}

chakra::error::Error chakra::net::Connect::setConnectTimeout() {
    timeval readv{}, writev{};
    toTimeVal(opts.readTimeOut, readv);
    toTimeVal(opts.writeTimeOut, writev);

    if (setsockopt(fd(), SOL_SOCKET, SO_RCVTIMEO, &readv, sizeof(readv))){
        return error::Error(strerror(errno));
    }

    if (setsockopt(fd(), SOL_SOCKET, SO_SNDTIMEO, &writev, sizeof(writev))){
        return error::Error(strerror(errno));
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
        return error::Error(strerror(errno));;
    }

    //  Returns the number of file descriptors with events, zero if timed out,
    //  or -1 for errors.
    int res;
    if ((res = poll(wfd, 1, msec)) == -1){
        close();
        return error::Error(strerror(errno));;
    } else if (res == 0){
        errno = ETIMEDOUT;
        close();
        return error::Error(strerror(errno));
    }

    auto err = checkConnectOK(res);
    if (err || res == 0)
        return checkSockErr();
    return error::Error();
}

chakra::error::Error chakra::net::Connect::checkSockErr() {
    int err1 = 0;
    int err_saved = errno;
    socklen_t err1len = sizeof(err1);
    if (getsockopt(fd(), SOL_SOCKET, SO_ERROR, &err1, &err1len) == -1){
        return error::Error(strerror(errno));
    }

    if (err1 == 0) err1 = err_saved;

    if (err1){
        return error::Error(strerror(err1));
    }
    return error::Error();
}

chakra::error::Error chakra::net::Connect::checkConnectOK(int &completed) {
    int rc = ::connect(fd(), const_cast<const struct sockaddr*>(sar), sizeof(*sar));
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
            return error::Error(strerror(errno));
    }
    return error::Error();
}

int chakra::net::Connect::fd() const { return FD; }

chakra::net::Connect::State chakra::net::Connect::connState() const { return state; }

long chakra::net::Connect::lastActiveTime() { return lastActive.time_since_epoch().count(); }

int chakra::net::Connect::reconnect() {
    Connect conn(opts);
    auto err = conn.connect();
    if (err) {
        return -1;
    }
    swap(*this, conn);
    return 0;
}

size_t chakra::net::Connect::sendBufferLength() { return wbuffer->len; }
size_t chakra::net::Connect::receiveBufferLength() { return rbuffer->len; }

void chakra::net::Connect::close() {
    if (FD == -1) return;
    ::close(FD);
    FD = -1;
    state = State::CLOSE;
}

chakra::net::Connect::~Connect() {
    close();
    if (sar != nullptr) { free(sar); sar =nullptr;}
    if (wbuffer != nullptr) { delete wbuffer; wbuffer = nullptr; } 
    if (rbuffer != nullptr) { delete rbuffer; rbuffer = nullptr; }
}

void chakra::net::swap(chakra::net::Connect &lc, chakra::net::Connect &rc) noexcept {
    std::swap(lc.opts, rc.opts);
    std::swap(lc.FD, rc.FD);
    std::swap(lc.sar, rc.sar);
    std::swap(lc.state, rc.state);
    std::swap(lc.lastActive, rc.lastActive);
}
