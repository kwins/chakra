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

class ClientSetBM : public benchmark::Fixture {
public:
    void SetUp(const benchmark::State& state) {
        chakra::client::Chakra::Options opts;
        opts.ip = "127.0.0.1";
        opts.maxConns = 100;
        opts.maxIdleConns = 10;
        try {
            client = std::make_shared<chakra::client::Chakra>(opts);
            // LOG(INFO) << "create client";
        } catch (const error::Error& err) {
            LOG(ERROR) << "create client error" << err.what();
            return;
        }
    }

    void TearDown(const ::benchmark::State& state) {
        client->close();
        // LOG(INFO) << "close client";
    }

    std::shared_ptr<chakra::client::Chakra> client;
};

BENCHMARK_DEFINE_F(ClientSetBM, case0)(benchmark::State& state) {
    proto::client::SetMessageResponse response;
    proto::client::SetMessageRequest request;
    request.set_db_name("db1");
    request.set_type(::proto::element::ElementType::STRING);
    request.set_ttl(2000);
    int64_t i = 0;
    int64_t spends = 0;
    for (auto _  : state) {
        request.set_key("bmcs2_key_" + std::to_string(i));
        request.set_s("bmcs2_value_" + std::to_string(i));
        auto err = client->set(request, response);
        if (err) LOG(ERROR) << err.what();
        i++;
    }
}

BENCHMARK_REGISTER_F(ClientSetBM, case0)->Iterations(300000)->Range(1, 100);

}