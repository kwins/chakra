#ifndef CHAKRA_UT_UTILS_H
#define CHAKRA_UT_UTILS_H
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include "utils/basic.h"
#include <glog/logging.h>
#include <google/protobuf/util/json_util.h>
#include "peer.pb.h"
#include "error/err.h"
#include "client.pb.h"
#include <malloc.h>
#include <list>

namespace chakra::unitest {

TEST(Utils, case0) {
    chakra::error::Error noerr("");
    chakra::error::FileError ferr("not exist");
    chakra::error::Error& rferr = ferr;

    ASSERT_EQ((noerr == true), false);
    ASSERT_EQ((noerr == false), true);
    
    ASSERT_EQ((ferr == true), true);
    ASSERT_EQ((ferr == false), false);

    ASSERT_EQ((rferr == true), true);
    ASSERT_EQ((rferr == false), false);
}

TEST(Utils, case1) {
    std::string ip = chakra::utils::Basic::getLocalIP();
    ASSERT_NE(ip, "");
    LOG(INFO) << "ip=" << ip;
}


TEST(Utils, case2) {
    std::list<int> q;
    q.push_back(1);
    q.push_back(2);
    q.push_back(3);
    q.push_back(4);
    q.push_back(5);
    q.push_back(6);

    // q.erase(std::remove_if(q.begin(), q.end(), [](int x){
    //     return x > 3;
    // }));

    q.remove_if([](int x){
        return x > 3;
    });

    for (auto v : q) {
        LOG(INFO) << "v:" << v;
    }
}

}
#endif //CHAKRA_UT_UTILS_H