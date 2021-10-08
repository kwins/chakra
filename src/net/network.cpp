//
// Created by kwins on 2021/5/21.
//

#include "network.h"

#include <glog/logging.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <fcntl.h>

chakra::error::Error chakra::net::Network::tpcListen(int port, int tcpBacklog, int& sockfd) {
    struct sockaddr_in addr{};
    int addr_len = sizeof(addr);
    int sd;
    if ((sd = ::socket(PF_INET, SOCK_STREAM, 0)) < 0){
        return error::Error(strerror(errno));
    }

    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (setSocketReuseAddr(sd) == -1){
        return error::Error(strerror(errno));;
    }

    if (::bind(sd, (const struct sockaddr*)&addr, addr_len) != 0){
        return error::Error(strerror(errno));;
    }

    if (::listen(sd, tcpBacklog) < 0){
        return error::Error(strerror(errno));;
    }
    sockfd = sd;
    return error::Error();
}


int chakra::net::Network::setSocketReuseAddr(int fd) {
    int yes = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) return -1;
    int flags;
    if ((flags = fcntl(fd, F_GETFL)) == -1) return -1;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) return -1;
    return 0;
}