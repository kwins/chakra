#ifndef CHAKRA_UT_CLIENT_SET_H
#define CHAKRA_UT_CLIENT_SET_H

#include <client.pb.h>
#include <element.pb.h>
#include <gtest/gtest.h>
#include <client/cplusplus/chakra_cluster.h>
#include <memory>
#include <glog/logging.h>
#include <string>

class ChakraClusterSetTest : public ::testing::Test {
protected:
    void SetUp() override {
        chakra::client::ChakraCluster::Options opts;
        opts.ip = "127.0.0.1";
        cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
    }
    void TearDown() override {}
    std::string prefixKey(std::string key) { return "mset9_" + key; }
    void clusterTestSet() {
        for (int i = 0; i < 10; i++) {
            std::string set_db = "db1";
            std::string set_key = "key_" + std::to_string(i);
            std::string set_value = "value_" + std::to_string(i);

            auto err = cluster->set(set_db, prefixKey(set_key), set_value, 200);
            ASSERT_EQ((err == false), true);
            proto::element::Element element;
            err = cluster->get(set_db, prefixKey(set_key), element, true);
            if (err) LOG(ERROR) << err.what();

            ASSERT_EQ((err == false), true);
            ASSERT_EQ(element.key(), prefixKey(set_key));
            ASSERT_EQ(element.s(), set_value);
            ASSERT_EQ(element.type(), proto::element::ElementType::STRING);
        }
    }

    void clusterTestMSetCase1() {
        proto::client::MSetMessageRequest msetMessageRequest;
        auto subSetReq1 = msetMessageRequest.mutable_datas()->Add();
        subSetReq1->set_db_name("db1");
        subSetReq1->set_key(prefixKey("key_1"));
        subSetReq1->set_type(::proto::element::ElementType::STRING);
        subSetReq1->set_s("value_1");

        auto subSetReq2 = msetMessageRequest.mutable_datas()->Add();
        subSetReq2->set_db_name("db1");
        subSetReq2->set_key(prefixKey("key_2"));
        subSetReq2->set_type(::proto::element::ElementType::STRING);
        subSetReq2->set_s("value_2");

        auto subSetReq3 = msetMessageRequest.mutable_datas()->Add();
        subSetReq3->set_db_name("db1");
        subSetReq3->set_key(prefixKey("key_3"));
        subSetReq3->set_type(::proto::element::ElementType::STRING);
        subSetReq3->set_s("value_3");

        proto::client::MSetMessageResponse msetMessageResponse;
        auto err = cluster->mset(msetMessageRequest, msetMessageResponse);
        EXPECT_TRUE((err == false));

        proto::client::MGetMessageRequest request;
        auto& db1 = (*request.mutable_keys())["db1"];
        db1.add_value(prefixKey("key_1"));
        db1.add_value(prefixKey("key_2"));
        db1.add_value(prefixKey("key_3"));

        proto::client::MGetMessageResponse response;
        err = cluster->mget(request, response, 10, true);
        EXPECT_TRUE((err == false));

        auto it = response.datas().find("db1");
        ASSERT_NE(it, response.datas().end());
        auto k1 = it->second.value().find(prefixKey("key_1"));
        ASSERT_EQ(k1, it->second.value().end());

        auto k2 = it->second.value().find(prefixKey("key_2"));
        ASSERT_EQ(k2, it->second.value().end());

        auto k3 = it->second.value().find(prefixKey("key_3"));
        ASSERT_EQ(k3, it->second.value().end());
    }

    void clusterTestMSetCase2() {
        proto::client::MSetMessageRequest msetMessageRequest;
        int num = 30;
        for (int i = 0; i < num; i++) {
            auto subSetReq = msetMessageRequest.mutable_datas()->Add();
            subSetReq->set_db_name("db1");
            subSetReq->set_key(prefixKey("key_" + std::to_string(i)));
            subSetReq->set_type(::proto::element::ElementType::STRING);
            subSetReq->set_s("value_" + std::to_string(i));
        }
        
        proto::client::MSetMessageResponse msetMessageResponse;
        auto err = cluster->mset(msetMessageRequest, msetMessageResponse);
        EXPECT_TRUE((err == false));
        
        LOG(INFO) << "mset request: " << msetMessageRequest.DebugString() << " response: " << msetMessageResponse.DebugString();
        
        proto::client::MGetMessageRequest mgetMessageRequest;
        for (int i = 0; i < num; i++) {
            auto& db1 = (*mgetMessageRequest.mutable_keys())["db1"];
            db1.add_value(prefixKey("key_") + std::to_string(i));
        }

        proto::client::MGetMessageResponse response;
        err = cluster->mget(mgetMessageRequest, response, 5, true);
        EXPECT_TRUE((err == false));
        
        LOG(INFO) << "mget request: " << mgetMessageRequest.DebugString() << " response: " << response.DebugString();
        
        auto it = response.datas().find("db1");
        ASSERT_NE(it, response.datas().end());
        for (int i = 0; i < num; i++) {
            auto kit = it->second.value().find(prefixKey("key_") + std::to_string(i));
            ASSERT_NE(kit, it->second.value().end());
        }
    }
private:
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};


TEST_F(ChakraClusterSetTest, case0) {
    clusterTestSet();
}

TEST_F(ChakraClusterSetTest, case1) {
    clusterTestMSetCase1();
}

TEST_F(ChakraClusterSetTest, case2) {
    clusterTestMSetCase2();
}

#endif // CHAKRA_UT_CLIENT_SET_H