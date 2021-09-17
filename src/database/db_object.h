//
// Created by kwins on 2021/6/18.
//

#ifndef CHAKRA_DB_OBJECT_H
#define CHAKRA_DB_OBJECT_H

#include <string>
#include <functional>
#include "types.pb.h"

namespace chakra::database{
class Object{
public:
    enum Type : uint8_t{
        STRING = 0,
        ARRAY = 1,
    };
public:
    virtual Type type() {return STRING;};
    virtual void serialize(const std::function<void(char* ptr, size_t len)>& f) {};
    virtual void deSeralize(const char* ptr, size_t len) {};
    virtual void debugString() {};
    virtual ~Object() = default;
};
}



#endif //CHAKRA_DB_OBJECT_H
