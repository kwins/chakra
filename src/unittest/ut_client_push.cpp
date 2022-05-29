#ifndef CHAKRA_UT_CLIENT_PUSH_H
#define CHAKRA_UT_CLIENT_PUSH_H

#include <client.pb.h>
#include <element.pb.h>
#include <gtest/gtest.h>
#include <client/cplusplus/chakra_cluster.h>
#include <memory>
#include <glog/logging.h>

namespace chakra::unitest {

class ClientPushTest : public ::testing::Test {
protected:
    void SetUp() override {
        chakra::client::ChakraCluster::Options opts;
        opts.ip = "127.0.0.1";
        cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
    }
    void TearDown() override {}

    //  方便换key
    std::string prefixKey(std::string key) { return "7_" + key; }
    
    void clusterPushCase0() {
        proto::client::MPushMessageRequest request;
        proto::client::MPushMessageResponse response;
        auto subReq1 = request.mutable_datas()->Add();
        subReq1->set_db_name("db1");
        subReq1->set_key(prefixKey("key_1"));
        subReq1->set_type(::proto::element::ElementType::STRING);
        subReq1->set_s("c1");
        
        auto subReq2 = request.mutable_datas()->Add();
        subReq2->set_db_name("db1");
        subReq2->set_key(prefixKey("key_2"));
        subReq2->set_type(::proto::element::ElementType::STRING);
        subReq2->set_s("c2");

        auto subReq3 = request.mutable_datas()->Add();
        subReq3->set_db_name("db1");
        subReq3->set_key(prefixKey("key_3"));
        subReq3->set_type(::proto::element::ElementType::STRING);
        subReq3->set_s("c3");

        auto subReq4 = request.mutable_datas()->Add();
        subReq4->set_db_name("db1");
        subReq4->set_key(prefixKey("key_4"));
        subReq4->set_type(::proto::element::ElementType::STRING);
        subReq4->set_s("c4");

        auto subReq5 = request.mutable_datas()->Add();
        subReq5->set_db_name("db1");
        subReq5->set_key(prefixKey("key_5"));
        subReq5->set_type(::proto::element::ElementType::STRING);
        subReq5->set_s("c5");

        auto subReq6 = request.mutable_datas()->Add();
        subReq6->set_db_name("db1");
        subReq6->set_key(prefixKey("key_6"));
        subReq6->set_type(::proto::element::ElementType::STRING);
        subReq6->set_s("c6");

        // -----------------------------------------------------------------
        auto err = cluster->mpush(request, 10, response);
        ASSERT_EQ((err == false), true);

        proto::client::GetMessageRequest getMessageRequest;
        getMessageRequest.set_db_name("db1");
        getMessageRequest.set_key(prefixKey("key_1"));
        proto::client::GetMessageResponse getMessageResponse;
        err = cluster->get(getMessageRequest, getMessageResponse, true);
        if (err) LOG(ERROR) << err.what();
        ASSERT_EQ((err == false), true);
        ASSERT_EQ(getMessageResponse.data().type(), proto::element::ElementType::STRING_ARRAY);
        ASSERT_EQ(getMessageResponse.data().ss().value(0), "c1");

        getMessageRequest.set_db_name("db1");
        getMessageRequest.set_key(prefixKey("key_2"));
        err = cluster->get(getMessageRequest, getMessageResponse, true);
        ASSERT_EQ((err == false), true);
        ASSERT_EQ(getMessageResponse.data().type(), proto::element::ElementType::STRING_ARRAY);
        ASSERT_EQ(getMessageResponse.data().ss().value(0), "c2");

        getMessageRequest.set_db_name("db1");
        getMessageRequest.set_key(prefixKey("key_3"));
        err = cluster->get(getMessageRequest, getMessageResponse, true);
        ASSERT_EQ((err == false), true);
        ASSERT_EQ(getMessageResponse.data().type(), proto::element::ElementType::STRING_ARRAY);
        ASSERT_EQ(getMessageResponse.data().ss().value(0), "c3");

        getMessageRequest.set_db_name("db1");
        getMessageRequest.set_key(prefixKey("key_4"));
        err = cluster->get(getMessageRequest, getMessageResponse, true);
        ASSERT_EQ((err == false), true);
        ASSERT_EQ(getMessageResponse.data().type(), proto::element::ElementType::STRING_ARRAY);
        ASSERT_EQ(getMessageResponse.data().ss().value(0), "c4");

        getMessageRequest.set_db_name("db1");
        getMessageRequest.set_key(prefixKey("key_5"));
        err = cluster->get(getMessageRequest, getMessageResponse, true);
        if (err) LOG(ERROR) << err.what();
        ASSERT_EQ((err == false), true);
        ASSERT_EQ(getMessageResponse.data().type(), proto::element::ElementType::STRING_ARRAY);
        ASSERT_EQ(getMessageResponse.data().ss().value(0), "c5");

        getMessageRequest.set_db_name("db1");
        getMessageRequest.set_key(prefixKey("key_6"));
        err = cluster->get(getMessageRequest, getMessageResponse, true);
        ASSERT_EQ((err == false), true);
        ASSERT_EQ(getMessageResponse.data().type(), proto::element::ElementType::STRING_ARRAY);
        ASSERT_EQ(getMessageResponse.data().ss().value(0), "c6");
    }
private:
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};

TEST_F(ClientPushTest, case0) {
    clusterPushCase0();
}

}
#endif // CHAKRA_UT_CLIENT_PUSH_H