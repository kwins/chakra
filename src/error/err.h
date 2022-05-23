//
// Created by kwins on 2021/10/8.
//

#ifndef CHAKRA_ERR_H
#define CHAKRA_ERR_H
#include <exception>
#include <string>
#include <stdexcept>

namespace chakra::error {

class Error : public std::logic_error {
public:
    explicit Error() : std::logic_error(""), message_("") {}
    explicit Error(const std::string& message) : std::logic_error(message), message_(message) {}
    virtual bool operator == (const bool value) { return (!message_.empty()) == value; }
    virtual operator bool() { return !message_.empty(); };
    virtual ~Error() noexcept override = default;
private:
    std::string message_;
};

#define DEFINE_ERROR(name, error)                                                                       \
class name##Error : public Error {                                                                      \
public:                                                                                                 \
    explicit name##Error(const std::string& message) : Error((std::string(#name) + ":" + message)) {}   \
    bool operator == (const bool value) override { return error == value; }                                      \
    operator bool() override { return error; }                                                          \
    ~name##Error() noexcept override = default;                                                         \
};

/* 这里定义的Error一般在try catch时使用，
 * 可以判断为某类错误，而不是使用错误码
 * 一般情况下就使用Error类即可
 */
DEFINE_ERROR(File, true)
DEFINE_ERROR(ConnectClosed, true)
DEFINE_ERROR(ConnectRead, true)

}

#endif //CHAKRA_ERR_H
