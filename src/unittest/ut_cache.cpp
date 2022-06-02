#ifndef CHAKRA_UT_CACHE_H
#define CHAKRA_UT_CACHE_H

#include <cstdint>
#include <element.pb.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <memory>
#include "database/db_column_cache.h"

namespace chakra::unitest {

TEST(ColumnDBLRUCache, set) {
    auto cache = std::make_shared<chakra::database::ColumnDBLRUCache>(20000);
    std::string key1 = "key_1";
    auto element = cache->get(key1);
    ASSERT_EQ(element, nullptr);

    cache->set(element, key1, "value_1");
    element = cache->get(key1);
    ASSERT_NE(element, nullptr);

    cache->set(element, key1, 10L);
    element = cache->get(key1);
    ASSERT_NE(element, nullptr);

    auto err = cache->push(element, key1, std::vector<float>{10.0,11.0,12.0});
    if (err) LOG(ERROR) << err.what();
    EXPECT_TRUE((err == true));
}

}
#endif // CHAKRA_UT_CACHE_H