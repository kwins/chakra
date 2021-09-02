//
// Created by kwins on 2021/6/30.
//

#ifndef CHAKRA_ERROR_H
#define CHAKRA_ERROR_H
#include <string>
namespace chakra::utils{
class Error {
public:
    explicit Error();
    Error(int err, std::string  msg);
    Error& operator = (const Error& err);
    Error(const Error& err);
    explicit operator bool () const;

    bool is(int code) const;
    int getCode() const;
    void setCode(int code);
    const std::string &getMsg() const;
    void setMsg(const std::string &msg);
    std::string toString();

    const static int ERR_PACK_NOT_ENOUGH = 1;
private:
    int code;
    std::string msg;
};
}



#endif //CHAKRA_ERROR_H
