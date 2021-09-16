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
//    explicit operator bool () const;

    bool is(int code) const;
    bool success() const;
    int getCode() const;
    void setCode(int code);
    const std::string &getMsg() const;
    void setMsg(const std::string &msg);
    std::string toString();

    const static int ERR_PACK_NOT_ENOUGH = 1;
    const static int ERR_PACK_TYPE = 2;
    const static int ERR_PACK_PARSE = 3;
    const static int ERR_BULK_TYPE = 4;
    const static int ERR_DATA_TYPE = 5;
    const static int ERR_FILE_NOT_EXIST = 6;
    const static int ERR_FILE_RENAME = 7;
    const static int ERR_FILE_CREATE = 8;
    const static int ERR_ERRNO = 999;
private:
    int code;
    std::string msg;
};
}



#endif //CHAKRA_ERROR_H
