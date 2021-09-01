//
// Created by kwins on 2021/6/30.
//

#include "error.h"

#include <utility>

chakra::utils::Error::Error() : code(0){}

chakra::utils::Error::Error(int err, std::string msg) : code(err), msg(std::move(msg)) {}

chakra::utils::Error &chakra::utils::Error::operator=(const chakra::utils::Error &err) = default;

chakra::utils::Error::Error(const chakra::utils::Error &err) {
    this->code = err.code;
    this->msg = err.msg;
}

chakra::utils::Error::operator bool() const {
    return code != 0;
}

std::string chakra::utils::Error::toString() {
    return "(" + std::to_string(code) + ":" + msg + ")";
}

int chakra::utils::Error::getCode() const {
    return code;
}

void chakra::utils::Error::setCode(int code) {
    Error::code = code;
}

const std::string &chakra::utils::Error::getMsg() const {
    return msg;
}

void chakra::utils::Error::setMsg(const std::string &msg) {
    Error::msg = msg;
}
