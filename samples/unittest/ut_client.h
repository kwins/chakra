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
#include <thread>
DEFINE_int32(port, 7290, "port");

class TestChakra : public ::testing::Test{
public:
protected:
    void SetUp() override {
        chakra::net::Connect::Options clientOpts;
        clientOpts.host = "127.0.0.1";
        clientOpts.port = 7290;
        client = std::make_shared<chakra::client::Chakra>(clientOpts);

        chakra::net::Connect::Options clusterOpts;
        clusterOpts.host = "127.0.0.1";
        clusterOpts.port = 7291;
        cluster = std::make_shared<chakra::client::Chakra>(clusterOpts);
    }

    void TearDown() override {
        client->close();
        cluster->close();
    }

    std::shared_ptr<chakra::client::Chakra> client;
    std::shared_ptr<chakra::client::Chakra> cluster;
};

TEST_F(TestChakra, client){
//    auto err = cluster->setdb("test_db", 100000);
//    if (!err.success()) LOG(ERROR) << "setdb:" << err.toString();
//
//     err = client->set("test_db", "key_1", "value_1");
//    if (!err.success()) LOG(ERROR) << "set:" << err.toString();
    std::string str;
    auto err = client->get("test_db", "key_1",str);
    if (!err.success()) LOG(ERROR) << "get:" << err.toString();
    else{
        LOG(INFO) << "get value:" << str;
    }
}

#endif //CHAKRA_UT_CLIENT_H
