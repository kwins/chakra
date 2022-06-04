#include <benchmark/benchmark.h>
#include <client/cplusplus/chakra.h>
#include <client/cplusplus/chakra_cluster.h>
#include <cstddef>
#include <cstdint>
#include <error/err.h>
#include <exception>
#include <gtest/gtest.h>
#include <string>
#include <glog/logging.h>

namespace chakra::bm {

class ClientSetBM : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) {
        chakra::client::Chakra::Options opts;
        opts.ip = "127.0.0.1";
        opts.maxConns = 100;
        opts.maxIdleConns = 50;
        try {
            client = std::make_shared<chakra::client::Chakra>(opts);
        } catch (const error::Error& err) {
            LOG(ERROR) << err.what();
        }
    }
    void TearDown(const ::benchmark::State& state) {
        if (client) client->close();
    }
    std::shared_ptr<chakra::client::Chakra> client;
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
        auto err = client->set(request, response);
        EXPECT_TRUE((err == false));
        ASSERT_EQ(response.error().errcode(), 0);
        i++;
    }
}

BENCHMARK_DEFINE_F(ClientSetBM, case1)(benchmark::State& st) {
    proto::client::SetMessageResponse response;
    proto::client::SetMessageRequest request;
    request.set_db_name("db1");
    request.set_type(::proto::element::ElementType::STRING);
    request.set_ttl(2000);
    int64_t i = 0;
    for (auto _  : st) {
        request.set_key("bm_key_" + std::to_string(i));
        request.set_s("bm_value_" + std::to_string(i));
        auto err = client->set(request, response);
        if (err) LOG(ERROR) << err.what();
        // EXPECT_TRUE((err == false));
        // ASSERT_EQ(response.error().errcode(), 0);
        i++;
    }
    LOG(INFO) << "error_occurred:" << st.error_occurred() << " items_processed:" << st.items_processed();
}

BENCHMARK_REGISTER_F(ClientSetBM, case1)->Threads(2);
}