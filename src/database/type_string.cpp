//
// Created by kwins on 2021/6/18.
//

#include "type_string.h"
#include <glog/logging.h>

chakra::database::Object::Type chakra::database::String::type() { return  chakra::database::Object::Type::STRING; }

void chakra::database::String::serialize(const std::function<void(char *, size_t)> &f) {
    f(str.data(), str.size());
}

void chakra::database::String::deSeralize(const char *ptr, size_t len) {
    str.assign(ptr, len);
}

void chakra::database::String::debugString() {
    LOG(INFO) << "STRING:" << str;
}
