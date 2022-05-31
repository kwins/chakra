//
// Created by kwins on 2021/5/21.
//

#ifndef CHAKRA_BASIC_H
#define CHAKRA_BASIC_H

#include <cstdint>
#include <string>
namespace chakra::utils {

class Basic {
public:
    static std::string genRandomID(size_t len = 40);
    static long getNowSec();
    static long getNowMillSec();
    static int32_t sport();
    static int32_t cport();
    static int32_t rport();
};

}



#endif //CHAKRA_BASIC_H
