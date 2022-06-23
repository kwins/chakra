#include <benchmark/benchmark.h>
#include <client/cplusplus/chakra.h>
#include <client/cplusplus/chakra_cluster.h>
#include <cstddef>
#include <cstdint>
#include <error/err.h>
#include <exception>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <glog/logging.h>
#include <utils/basic.h>

namespace chakra::bm {

class ClientClusterSetBM : public benchmark::Fixture {
public:
    void SetUp(const benchmark::State& state) {
        chakra::client::ChakraCluster::Options opts;
        opts.ip = "10.210.16.240";
        opts.maxConns = 100;
        opts.maxIdleConns = 10;
        try {
            cluster = std::make_shared<chakra::client::ChakraCluster>(opts);
            // LOG(INFO) << "create cluster";
        } catch (const error::Error& err) {
            LOG(ERROR) << "create client error" << err.what();
            return;
        }
    }

    void TearDown(const ::benchmark::State& state) {
        cluster->close();
        // LOG(INFO) << "close cluster";
    }

    std::shared_ptr<chakra::client::ChakraCluster> cluster;
};

BENCHMARK_DEFINE_F(ClientClusterSetBM, case0)(benchmark::State& state) {
    proto::client::SetMessageResponse response;
    proto::client::SetMessageRequest request;
    request.set_db_name("db1");
    request.set_type(::proto::element::ElementType::STRING);
    request.set_ttl(2000);
    int64_t i = 0;
    int64_t spends = 0;
    for (auto _  : state) {
        request.set_key("bm4_key_" + std::to_string(i));
        request.set_s("bm4_value_" + std::to_string(i));
        auto err = cluster->set(request, response);
        if (err) LOG(ERROR) << err.what();
        i++;
    }
}

BENCHMARK_REGISTER_F(ClientClusterSetBM, case0)->Iterations(500000)->Range(1, 100);

}