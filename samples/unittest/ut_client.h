//
// Created by kwins on 2021/5/14.
//

#ifndef CHAKRA_UT_CLIENT_H
#define CHAKRA_UT_CLIENT_H

#include <gtest/gtest.h>
#include <iostream>
#include "client/cplusplus/chakra.h"
#include "net/connect.h"
#include <glog/logging.h>

DEFINE_int32(port, 7290, "port");

TEST(Chakra, client){
    chakra::net::Connect::Options options;
    options.host = "127.0.0.1";
    chakra::client::Chakra client(options);
    auto reply = client.meet("127.0.0.1", FLAGS_port);
    LOG(INFO) << "reply:" << reply.DebugString();
}

#endif //CHAKRA_UT_CLIENT_H
