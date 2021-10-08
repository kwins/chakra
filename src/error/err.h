//
// Created by kwins on 2021/10/8.
//

#ifndef CHAKRA_ERR_H
#define CHAKRA_ERR_H
#include <exception>
#include <string>

namespace chakra::error {

class Error : public std::logic_error{
public:
    explicit Error();
    explicit Error(const char* message);
    explicit Error(const std::string& message);
    bool success() const;
    int getCode() const;
    const std::string &getMsg() const;
    ~Error() noexcept override = default;

private:
    int errcode;
    std::string errmsg;
};

}

#endif //CHAKRA_ERR_H
