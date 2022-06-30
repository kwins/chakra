#include <benchmark/benchmark.h>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include "database/db_column_cache.h"
#include <glog/logging.h>

namespace chakra::bm {
class DBCacheErase : public benchmark::Fixture { 
public:
    DBCacheErase() {
        cache = std::make_shared<database::ColumnDBLRUCache>(1000000);
        for (int i = 0; i < 100000; i++) {
            keys.push_back("key_" + std::to_string(i));
        }
        for (int i = 0; i < 2500; i++) {
            std::set<std::string> ks;
            for (int j = 0; j < 4; j++) {
                ks.emplace("key_" + std::to_string(j));
            }
            groupKeys.push_back(ks);
        }
    }

    std::vector<std::string> keys;
    std::vector<std::set<std::string>> groupKeys;
    std::shared_ptr<database::ColumnDBLRUCache> cache;
};

BENCHMARK_DEFINE_F(DBCacheErase, case0)(benchmark::State& state) {
    int i = 0;
    for (auto _  : state) {
        cache->erase(keys[i]);
        i++;
    }
}

BENCHMARK_REGISTER_F(DBCacheErase, case0)->Iterations(100000)->Threads(100)->UseRealTime();

BENCHMARK_DEFINE_F(DBCacheErase, case1)(benchmark::State& state) {
    int i = 0;
    for (auto _  : state) {
        cache->erase(groupKeys[i]);
        i++;
    }
}

BENCHMARK_REGISTER_F(DBCacheErase, case1)->Iterations(2500)->Threads(100)->UseRealTime();

}