//
// Created by kwins on 2021/5/21.
//

#include "basic.h"

#include <chrono>
#include <random>
#include <algorithm>

std::string chakra::utils::Basic::genRandomID(size_t len) {
    if (len < 16) len = 16;
    static const char alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    std::random_device rd;
    std::default_random_engine rng(rd());
    std::uniform_int_distribution<> dist(0,25);

    std::string str;
    std::generate_n(std::back_inserter(str), len - 14, [&]{
       return alphabet[dist(rng)];
    });

    return std::to_string(getNowMillSec()) + "-" + str;
}

long chakra::utils::Basic::getNowSec() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::seconds>(now).time_since_epoch().count();

}

long chakra::utils::Basic::getNowMillSec() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();
}
