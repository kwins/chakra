#include <client.pb.h>
#include <element.pb.h>
#include <gtest/gtest.h>
#include <client/cplusplus/chakra_cluster.h>
#include <memory>
#include <glog/logging.h>
#include <string>

namespace chakra::unitest {


class ClientScanTest : public ::testing::Test {
protected:
    void SetUp() override {
        chakra::client::ChakraCluster::Options opts;
        opts.ip = "127.0.0.1";
        cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
    }
    void TearDown() override {}

    void clientScanTestCase0() {
        proto::client::ScanMessageRequest scanMessageRequest;
        scanMessageRequest.set_dbname("db1");
        scanMessageRequest.set_batch_size(10);

        proto::client::ScanMessageResponse scanMessageResponse;
        bool end = false;
        while (!end) {
            auto err = cluster->scan(scanMessageRequest, scanMessageResponse);
            if (err) {
                LOG(ERROR) << err.what();
                break;
            } else {
                for (auto& element : scanMessageResponse.elements()) {
                    LOG(INFO) << element.DebugString();
                }
                LOG(INFO) << "------> last key:" << scanMessageResponse.elements(scanMessageResponse.elements_size()-1).key();
                scanMessageRequest.set_peername(scanMessageResponse.peername());
                scanMessageRequest.set_start(scanMessageResponse.elements(scanMessageResponse.elements_size()-1).key());
                end = scanMessageResponse.end();
            }
        }
    }
private:
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};

TEST_F(ClientScanTest, case0) {
    clientScanTestCase0();
}

}