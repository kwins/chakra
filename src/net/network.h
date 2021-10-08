//
// Created by kwins on 2021/5/21.
//

#ifndef CHAKRA_NETWORK_H
#define CHAKRA_NETWORK_H
#include "error/err.h"

namespace chakra::net{

class Network {
public:
    static error::Error tpcListen(int port, int tcpBacklog, int& sockfd);
    static int setSocketReuseAddr(int fd);
};


}



#endif //CHAKRA_NETWORK_H
