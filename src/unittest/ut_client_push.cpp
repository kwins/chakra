#ifndef CHAKRA_UT_CLIENT_H
#define CHAKRA_UT_CLIENT_H

#include <gtest/gtest.h>
#include <client/cplusplus/chakra_cluster.h>
#include <memory>
#include <glog/logging.h>

class ChakraClusterPushTest : public ::testing::Test {
protected:
    void SetUp() override {
        chakra::client::ChakraCluster::Options opts;
        opts.ip = "127.0.0.1";
        cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
    }
    void TearDown() override {}

    void clusterTestPush() {
        proto::client::MPushMessageRequest request;
        proto::client::MPushMessageResponse response;
        auto subReq1 = request.mutable_datas()->Add();
        subReq1->set_db_name("db1");
        subReq1->set_key("key_1");
        subReq1->set_type(::proto::element::ElementType::STRING);
        subReq1->set_s("c1");
        
        auto subReq2 = request.mutable_datas()->Add();
        subReq2->set_db_name("db1");
        subReq2->set_key("key_1");
        subReq2->set_type(::proto::element::ElementType::STRING);
        subReq2->set_s("c2");

        auto subReq3 = request.mutable_datas()->Add();
        subReq3->set_db_name("db1");
        subReq3->set_key("key_2");
        subReq3->set_type(::proto::element::ElementType::STRING);
        subReq3->set_s("c3");

        auto subReq4 = request.mutable_datas()->Add();
        subReq4->set_db_name("db1");
        subReq4->set_key("key_3");
        subReq4->set_type(::proto::element::ElementType::STRING);
        subReq4->set_s("c4");

        auto subReq5 = request.mutable_datas()->Add();
        subReq5->set_db_name("db2");
        subReq5->set_key("key_4");
        subReq5->set_type(::proto::element::ElementType::STRING);
        subReq5->set_s("c5");

        auto subReq6 = request.mutable_datas()->Add();
        subReq6->set_db_name("db2");
        subReq6->set_key("key_4");
        subReq6->set_type(::proto::element::ElementType::STRING);
        subReq6->set_s("c6");

        LOG(INFO) << "request: " << request.DebugString();
        auto err = cluster->mpush(request, 10, response);
        ASSERT_EQ((err == false), true);

        proto::element::Element element;
        err = cluster->get("db1", "key_1", element);
        ASSERT_EQ((err == false), true);
        LOG(INFO) << "response: " << element.DebugString();
    }
private:
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};

TEST_F(ChakraClusterPushTest, mpush) {
    clusterTestPush();
}

#endif // CHAKRA_UT_CLIENT_H