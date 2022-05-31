#include <client.pb.h>
#include <client/cplusplus/chakra.h>
#include <element.pb.h>
#include <gtest/gtest.h>
#include <client/cplusplus/chakra_cluster.h>
#include <memory>
#include <glog/logging.h>
#include <string>

namespace chakra::unitest {

class ClientStateTest : public ::testing::Test {
protected:
    void SetUp() override {
        // chakra::client::ChakraCluster::Options opts;
        // cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
        chakra::client::Chakra::Options opts;
        opts.ip = "127.0.0.1";
        client = std::make_shared<chakra::client::Chakra>(opts);
    }
    void TearDown() override {}
    void testClientStateCase0() {
        proto::peer::ClusterState clusterState;
        auto err = client->state(clusterState);
        EXPECT_TRUE((err==false));
        LOG(INFO) << "cluster state: " << clusterState.DebugString();
    }

private:
    std::shared_ptr<chakra::client::Chakra> client;
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};


TEST_F(ClientStateTest, case0) {
    testClientStateCase0();
}

}