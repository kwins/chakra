//
// Created by kwins on 2021/10/8.
//

#include "err.h"

chakra::error::Error::Error() : std::logic_error(""), errcode(0), errmsg("ok") {}

chakra::error::Error::Error(const char *message) : logic_error(message), errcode(1), errmsg(message) {}

chakra::error::Error::Error(const std::string &message) : logic_error(message), errcode(1), errmsg(message) {}

bool chakra::error::Error::success() const { return errcode == 0; }

int chakra::error::Error::getCode() const { return errcode; }

const std::string &chakra::error::Error::getMsg() const { return errmsg; }
