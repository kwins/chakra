#include <client.pb.h>
#include <element.pb.h>
#include <gtest/gtest.h>
#include <client/cplusplus/chakra_cluster.h>
#include <memory>
#include <glog/logging.h>
#include <string>

namespace chakra::unitest {

class ClientSetTest : public ::testing::Test {
protected:
    void SetUp() override {
        chakra::client::ChakraCluster::Options opts;
        opts.ip = "127.0.0.1";
        cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
    }
    void TearDown() override {}
    std::string prefixKey(std::string key) { return "set13_" + key; }

    void testClientSetCase0() {
        proto::client::SetMessageRequest setMessageRequest;
        proto::client::SetMessageResponse setMessageResponse;

        proto::client::GetMessageRequest getMessageRequest;
        proto::client::GetMessageResponse getMessageResponse;
        for (int i = 0; i < 1; i++) {
            std::string set_db = "db1";
            std::string set_key = prefixKey("set_get_1");
            std::string set_value = prefixKey("set_get_value_1");
            
            setMessageRequest.set_db_name(set_db);
            setMessageRequest.set_key(set_key);
            setMessageRequest.set_type(::proto::element::ElementType::STRING);
            setMessageRequest.set_s( set_value);
            setMessageRequest.set_ttl(200);
            auto err = cluster->set(setMessageRequest, setMessageResponse);
            ASSERT_EQ((err == false), true);

            getMessageRequest.set_db_name(set_db);
            getMessageRequest.set_key(set_key);
            err = cluster->get(getMessageRequest, getMessageResponse, true);
            if (err) LOG(ERROR) << err.what();

            ASSERT_EQ((err == false), true);
            ASSERT_EQ(getMessageResponse.data().key(), set_key);
            ASSERT_EQ(getMessageResponse.data().s(), set_value);
            ASSERT_EQ(getMessageResponse.data().type(), proto::element::ElementType::STRING);
        }
    }

    void testClientSetCase1() {
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
        ASSERT_NE(k1, it->second.value().end());

        auto k2 = it->second.value().find(prefixKey("key_2"));
        ASSERT_NE(k2, it->second.value().end());

        auto k3 = it->second.value().find(prefixKey("key_3"));
        ASSERT_NE(k3, it->second.value().end());
    }

    void testClientSetCase3() {
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


TEST_F(ClientSetTest, case0) {
    testClientSetCase0();
}

TEST_F(ClientSetTest, case1) {
    testClientSetCase1();
}

TEST_F(ClientSetTest, case2) {
    testClientSetCase3();
}

}