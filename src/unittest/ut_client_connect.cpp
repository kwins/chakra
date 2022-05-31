#include <client.pb.h>
#include <element.pb.h>
#include <gtest/gtest.h>
#include <client/cplusplus/chakra_cluster.h>
#include <memory>
#include <glog/logging.h>
#include <string>

namespace chakra::unitest {

class ClientConnectTest : public ::testing::Test {
protected:
    void SetUp() override {
        chakra::client::ChakraCluster::Options opts;
        opts.ip = "127.0.0.1";
        cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
    }
    void TearDown() override {}
    void testClientConnectCase0() {}

private:
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};


TEST_F(ClientConnectTest, case0) {
    testClientConnectCase0();
}

}