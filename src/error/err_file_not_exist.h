//
// Created by kwins on 2021/10/8.
//

#ifndef CHAKRA_ERR_FILE_NOT_EXIST_H
#define CHAKRA_ERR_FILE_NOT_EXIST_H

#include <exception>
#include <string>
#include <stdexcept>

namespace chakra::error{

class FileNotExistError : public std::logic_error {
public:
    explicit FileNotExistError(const char* message);
    explicit FileNotExistError(const std::string& message);
    ~FileNotExistError() noexcept override = default;
};

}



#endif //CHAKRA_ERR_FILE_NOT_EXIST_H
