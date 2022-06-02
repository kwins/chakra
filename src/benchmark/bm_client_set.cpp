#include <benchmark/benchmark.h>
#include <client/cplusplus/chakra_cluster.h>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>

namespace chakra::bm {

class ClientSetBM : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) {
        chakra::client::ChakraCluster::Options opts;
        opts.ip = "127.0.0.1";
        cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
    }
    void TearDown(const ::benchmark::State& state) {
        cluster->close();
    }
    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};

BENCHMARK_F(ClientSetBM, case0)(benchmark::State& st) {
    proto::client::SetMessageResponse response;
    proto::client::SetMessageRequest request;
    request.set_db_name("db1");
    request.set_type(::proto::element::ElementType::STRING);
    request.set_ttl(2000);
    int64_t i = 0;
    for (auto _  : st) {
        request.set_key("bm_key_" + std::to_string(i));
        request.set_s("bm_value_" + std::to_string(i));
        auto err = cluster->set(request, response);
        EXPECT_TRUE((err == false));
        ASSERT_EQ(response.error().errcode(), 0);
        i++;
    }
}

}