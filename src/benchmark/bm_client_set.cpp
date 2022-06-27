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
#include <vector>

namespace chakra::bm {

class ClientSetBM : public benchmark::Fixture {
public:
    ClientSetBM() { 
        clients.resize(threads); 
        for (int i = 0; i < threads; i++) {
            chakra::client::Chakra::Options opts;
            opts.ip = "10.210.16.240";
            opts.port = 9290;
            opts.maxConns = 10;
            opts.maxIdleConns = 3;
            try {
                clients[i] = std::make_shared<chakra::client::Chakra>(opts);
                // LOG(INFO) << "create client";
            } catch (const error::Error& err) {
                LOG(ERROR) << "create client error " << err.what();
                return;
            }
        }
    }

    // 多线程共用一个对象，但是每个线程都会执行 SetUp 和 TearDown 函数。
    void SetUp(const benchmark::State& state) {
    }

    void TearDown(const ::benchmark::State& state) {
    }

    std::vector<std::shared_ptr<chakra::client::Chakra>> clients;

    static int threads;
};

int ClientSetBM::threads = 1000;

BENCHMARK_DEFINE_F(ClientSetBM, case0)(benchmark::State& state) {
    proto::client::SetMessageResponse response;
    proto::client::SetMessageRequest request;
    request.set_db_name("db1");
    request.set_type(::proto::element::ElementType::STRING);
    request.set_ttl(2000);
    int64_t i = 0;
    int64_t spends = 0;
    for (auto _  : state) {
        request.set_key("bmcs5_key_" + std::to_string(i));
        request.set_s("bmcs5_value_" + std::to_string(i));
        auto err = clients[state.thread_index()]->set(request, response);
        if (err) LOG(ERROR) << err.what();
        i++;
    }
}

BENCHMARK_REGISTER_F(ClientSetBM, case0)->Iterations(100000)->ThreadRange(8, ClientSetBM::threads);
}