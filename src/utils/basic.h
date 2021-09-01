//
// Created by kwins on 2021/5/21.
//

#ifndef CHAKRA_BASIC_H
#define CHAKRA_BASIC_H

#include <string>
namespace chakra::utils{

class Basic {
public:
//    static std::string genRandomID(unsigned int len = 40);
    static std::string genRandomID(size_t len = 40);
    static long getNowSec();
    static long getNowMillSec();
};

}



#endif //CHAKRA_BASIC_H
