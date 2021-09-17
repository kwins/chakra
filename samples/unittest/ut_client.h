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
    void SetUp() override {
        int port = 9290;
        chakra::net::Connect::Options clientOpts;
        clientOpts.host = "127.0.0.1";
        clientOpts.port = port;
        client = std::make_shared<chakra::client::Chakra>(clientOpts);

        chakra::net::Connect::Options clusterOpts;
        clusterOpts.host = "127.0.0.1";
        clusterOpts.port = port + 1;
        clusterptr = std::make_shared<chakra::client::Chakra>(clusterOpts);

        chakra::net::Connect::Options replicaOpts;
        replicaOpts.host = "127.0.0.1";
        replicaOpts.port = port + 2;
        replicaptr = std::make_shared<chakra::client::Chakra>(clusterOpts);
    }

    void testSetDB(const std::string& dbname){
        auto err = clusterptr->setdb(dbname, 100000);
        if (!err.success()) LOG(ERROR) << "setdb:" << err.toString();
        else
            LOG(ERROR) << "setdb success";
    }

    void testSetKeyValue(const std::string& dbname, const std::string& key, const std::string& value){
        auto err = client->set(dbname, key, value);
        if (!err.success()) LOG(ERROR) << "set:" << err.toString();
        else
            LOG(ERROR) << "set key " << key << " value " << value << " success.";
    }

    void testGetValue(const std::string& dbname, const std::string& key){
        std::string str;
        auto err = client->get(dbname, key,str);
        if (!err.success()) LOG(ERROR) << "get:" << err.toString();
        else{
            LOG(INFO) << "get value:" << str;
        }
    }

    void testReplicaOf(const std::string& dbname){
        auto err = replicaptr->replicaof(dbname);
        if (!err.success())
            LOG(ERROR) << "replica of " << err.toString();
        else
            LOG(ERROR) << "replica of success";
    }

    void testMeet(const std::string& ip, int port){
        auto err = clusterptr->meet(ip, port);
        if (!err.success()) LOG(ERROR) << "meet error " << err.toString();
        else
            LOG(ERROR) << "meet success";
    }

    void TearDown() override {
        client->close();
        clusterptr->close();
        replicaptr->close();
    }

    std::shared_ptr<chakra::client::Chakra> client;
    std::shared_ptr<chakra::client::Chakra> clusterptr;
    std::shared_ptr<chakra::client::Chakra> replicaptr;
};

TEST_F(TestChakra, client){
//    testReplicaOf("db1");
//    testMeet("127.0.0.1", 9291);
//    std::this_thread::sleep_for(std::chrono::seconds(10));
    testSetDB("db5");
//    testSetKeyValue("db1", "db1_key1", "db1_value1");
//    testGetValue("db1", "db1_key1");
//testReplicaOf("db1");
}

#endif //CHAKRA_UT_CLIENT_H
