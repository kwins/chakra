#include <client.pb.h>
#include <element.pb.h>
#include <gtest/gtest.h>
#include <client/cplusplus/chakra_cluster.h>
#include <memory>
#include <glog/logging.h>
#include <string>

namespace chakra::unitest {

class ClientIncrTest : public ::testing::Test {
protected:
    void SetUp() override {
        chakra::client::ChakraCluster::Options opts;
        opts.ip = "127.0.0.1";
        cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
    }
    void TearDown() override {}
    std::string prefixKey(std::string key) { return "incr4_" + key; }

    void clientIncrTestCase0() {
        std::string set_db = "db1";
        std::string set_key = prefixKey("key_1");
        float set_value = 1;
        float incr_value = 2;
        proto::client::SetMessageRequest setMessageRequest;
        setMessageRequest.set_db_name(set_db);
        setMessageRequest.set_key(set_key);
        setMessageRequest.set_type(proto::element::ElementType::FLOAT);
        setMessageRequest.set_f( set_value);
        setMessageRequest.set_ttl(200);
        LOG(INFO) << "set request: " << setMessageRequest.DebugString();
        proto::client::SetMessageResponse setMessageResponse;
        auto err = cluster->set(setMessageRequest, setMessageResponse);
        if (err) LOG(ERROR) << err.what();
        ASSERT_EQ((err == false), true);


        proto::client::IncrMessageRequest incrMessageRequest;
        incrMessageRequest.set_db_name(set_db);
        incrMessageRequest.set_key(set_key);
        incrMessageRequest.set_type(::proto::element::ElementType::FLOAT);
        incrMessageRequest.set_f( incr_value);
        incrMessageRequest.set_ttl(200);

        proto::client::IncrMessageResponse incrMessageResponse;
        err = cluster->incr(incrMessageRequest, incrMessageResponse);
        if (err) LOG(ERROR) << err.what();
        ASSERT_EQ((err == false), true);

        proto::client::GetMessageRequest getMessageRequest;
        getMessageRequest.set_db_name(set_db);
        getMessageRequest.set_key(set_key);

        proto::client::GetMessageResponse getMessageResponse;
        err = cluster->get(getMessageRequest, getMessageResponse, true);
        if (err) LOG(ERROR) << err.what();
        ASSERT_EQ((err == false), true);
        LOG(INFO) << getMessageResponse.DebugString();
        ASSERT_EQ(getMessageResponse.data().f(), 3);
    }

    void clientIncrTestCase1() {
        // set
        proto::client::MSetMessageRequest msetMessageRequest;
        int num = 30;
        for (int i = 0; i < num; i++) {
            auto subSetReq = msetMessageRequest.mutable_datas()->Add();
            subSetReq->set_db_name("db1");
            subSetReq->set_key(prefixKey("key_" + std::to_string(i)));
            subSetReq->set_type(::proto::element::ElementType::FLOAT);
            subSetReq->set_f(1);
        }
        
        proto::client::MSetMessageResponse msetMessageResponse;
        auto err = cluster->mset(msetMessageRequest, msetMessageResponse);
        if (err) LOG(ERROR) << err.what();
        EXPECT_TRUE((err == false));

        for (int i = 0; i < num; i++) {
            auto& db = msetMessageResponse.states().at("db1");
            auto ki = db.value().find(prefixKey("key_" + std::to_string(i)));
            ASSERT_NE(ki, db.value().end());
            EXPECT_TRUE(ki->second.succ());
        }
        // incr
        proto::client::MIncrMessageRequest mincrMessageRequest;
        for (int i = 0; i < num; i++) {
            auto sub = mincrMessageRequest.mutable_datas()->Add();
            sub->set_db_name("db1");
            sub->set_key(prefixKey("key_" + std::to_string(i)));
            sub->set_type(::proto::element::ElementType::FLOAT);
            sub->set_ttl(200);
            sub->set_f(1);
        }
        proto::client::MIncrMessageResponse mincrMessageResponse;
        err = cluster->mincr(mincrMessageRequest, mincrMessageResponse);
        if (err) LOG(ERROR) << err.what();
        EXPECT_TRUE((err == false));

        // get verify
        proto::client::MGetMessageRequest mgetMessageRequest;
        for (int i = 0; i < num; i++) {
            auto& sub = (*mgetMessageRequest.mutable_keys())["db1"];
            sub.add_value(prefixKey("key_" + std::to_string(i)));
        }
        proto::client::MGetMessageResponse mgetMessageResponse;
        err = cluster->mget(mgetMessageRequest, mgetMessageResponse, 10, true);
        if (err) LOG(ERROR) << err.what();
        EXPECT_TRUE((err == false));
        auto db = mgetMessageResponse.datas().find("db1");
        ASSERT_NE(db, mgetMessageResponse.datas().end());
        for (int i = 0; i < num; i++) {
            auto ki = db->second.value().find(prefixKey("key_" + std::to_string(i)));
            ASSERT_NE(ki, db->second.value().end());
            ASSERT_EQ(ki->second.type(), proto::element::ElementType::FLOAT);
            ASSERT_EQ(ki->second.f(), 2);
        }
    }
private:
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};

TEST_F(ClientIncrTest, case0) {
    clientIncrTestCase0();
}

TEST_F(ClientIncrTest, case1) {
    clientIncrTestCase1();
}
}