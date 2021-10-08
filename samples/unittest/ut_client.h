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
        int port = 8290;
        chakra::net::Connect::Options clientOpts;
        clientOpts.host = "127.0.0.1";
        clientOpts.port = port;
        client = std::make_shared<chakra::client::Chakra>(clientOpts);

        chakra::net::Connect::Options clusterOpts;
        clusterOpts.host = "127.0.0.1";
        clusterOpts.port = port + 1;
        clusterptr = std::make_shared<chakra::client::Chakra>(clusterOpts);
    }

    void testSetDB(const std::string& dbname){
        auto err = clusterptr->setdb(dbname, 100000);
        if (!err.success()) LOG(ERROR) << "setdb:" << err.what();
        else
            LOG(ERROR) << "setdb success";
    }

    void testSetKeyValue(const std::string& dbname, const std::string& key, const std::string& value){
        auto err = client->set(dbname, key, value);
        if (!err.success()) LOG(ERROR) << "set:" << err.what();
        else
            LOG(ERROR) << "set key " << key << " value " << value << " success.";
    }

    void testGetValue(const std::string& dbname, const std::string& key){
        std::string str;
        auto err = client->get(dbname, key,str);
        if (!err.success()) LOG(ERROR) << "get:" << err.what();
        else{
            LOG(INFO) << "get key " << key << " value:" << str;
        }
    }

    void testMeet(const std::string& ip, int port){
        auto err = clusterptr->meet(ip, port);
        if (!err.success()) LOG(ERROR) << "meet error " << err.what();
        else
            LOG(ERROR) << "meet success";
    }

    void testState(){
        proto::peer::ClusterState clusterState;
        auto err = clusterptr->state(clusterState);
        if (!err.success())  LOG(ERROR) << "state error " << err.what();
        else
            LOG(ERROR) << "state: " << clusterState.DebugString();
    }

    void TearDown() override {
        client->close();
        clusterptr->close();
    }

    void testSetEpoch(){
        auto err = clusterptr->setEpoch(0, true);
        if (!err.success())  LOG(ERROR) << "set epoch error " << err.what();
        else
            LOG(ERROR) << "set epoch success.";
    }
    std::shared_ptr<chakra::client::Chakra> client;
    std::shared_ptr<chakra::client::Chakra> clusterptr;
    std::shared_ptr<chakra::client::Chakra> replicaptr;
};

TEST_F(TestChakra, client){
//    testReplicaOf("db1");
//    testMeet("127.0.0.1", 8291);
//    testMeet("127.0.0.1", 9291);

//testState();
//    std::this_thread::sleep_for(std::chrono::seconds(10));
//    testSetDB("db1");
//    testSetEpoch();
//for (int i = 600; i < 700; i ++){
//    std::string is = std::to_string(i);
//    testSetKeyValue("db1", "key" + is, "value" + is);
//}

for (int i = 600; i < 700; i ++){
    std::string is = std::to_string(i);
    testGetValue("db1", "key" + is);
}

//testReplicaOf("db1");
}

#endif //CHAKRA_UT_CLIENT_H
