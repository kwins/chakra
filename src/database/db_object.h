//
// Created by kwins on 2021/6/18.
//

#ifndef CHAKRA_DB_OBJECT_H
#define CHAKRA_DB_OBJECT_H

#include <string>

namespace chakra::database{
class Object{
public:
    enum Type : uint8_t{
        STRING,
        ARRAY,
    };
public:
    virtual Type type() = 0;
    virtual void serialize(const std::function<void(char* ptr, size_t len)>& f) = 0;
    virtual void deSeralize(const char* ptr, size_t len) = 0;
    virtual void debugString() = 0;
    virtual ~Object() = default;
};
}



#endif //CHAKRA_DB_OBJECT_H
