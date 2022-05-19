#ifndef CHAKRA_UT_CACHE_H
#define CHAKRA_UT_CACHE_H

#include <cstdint>
#include <element.pb.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <memory>
#include "database/db_column_cache.h"

TEST(ColumnDBLRUCache, set) {
    auto cache = std::make_shared<chakra::database::ColumnDBLRUCache>(200);
    std::string key1 = "key_1";
    cache->set(key1, "value_1");
    auto element = cache->get(key1);
    ASSERT_NE(element, nullptr);

    cache->set(key1, 10L);
    element = cache->get(key1);
    ASSERT_NE(element, nullptr);

    cache->push(key1, std::vector<int64_t>{10L,11L,12L});
    element = cache->get(key1);
    ASSERT_NE(element, nullptr);
    ASSERT_EQ(element->type(), proto::element::ElementType::INTEGER_ARRAY);

    auto err = cache->incr(key1, (int64_t)2.0L);
    ASSERT_EQ((err == true), true);

    err = cache->incr(key1, (float)2.0);
    ASSERT_EQ((err == true), true);
}

#endif // CHAKRA_UT_CACHE_H