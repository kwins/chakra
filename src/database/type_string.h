//
// Created by kwins on 2021/6/18.
//

#ifndef CHAKRA_TYPE_STRING_H
#define CHAKRA_TYPE_STRING_H

#include "element.h"

namespace chakra::database{
class String : public Object {
public:
    Type type() override;

    void serialize(const std::function<void(char *, size_t)> &f) override;

    void deSeralize(const char *ptr, size_t len) override;

    void debugString() override;

private:
    std::string str;
};

}



#endif //CHAKRA_TYPE_STRING_H
