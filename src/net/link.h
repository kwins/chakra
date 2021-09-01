//
// Created by kwins on 2021/6/29.
//

#ifndef CHAKRA_LINK_H
#define CHAKRA_LINK_H

#include "connect.h"

namespace chakra::net{
class Link {
public:
    //    void sendSyncMsg()
private:
    std::shared_ptr<net::Connect> conn;
};

}



#endif //CHAKRA_LINK_H
