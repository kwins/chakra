//
// Created by kwins on 2021/10/8.
//

#include "err_file_not_exist.h"

chakra::error::FileNotExistError::FileNotExistError(const std::string& message)
        : std::logic_error(message) {}

chakra::error::FileNotExistError::FileNotExistError(const char *message) : std::logic_error(message) {}