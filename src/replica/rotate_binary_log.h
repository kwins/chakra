//
// Created by kwins on 2021/6/24.
//

#ifndef CHAKRA_ROTATE_BINARY_LOG_H
#define CHAKRA_ROTATE_BINARY_LOG_H
#include <string>
#include <iostream>
#include <memory>

namespace chakra::replica{
class RotateBinaryLog {
public:
    struct Position{
        std::string name;
        int64_t offset;
    };
    struct Options{
        std::string dir;
        uint64_t rotateLogSize;
    };
public:
    explicit RotateBinaryLog(const Options& opts);
    uint64_t write(const char* s, size_t len);
    Position curPosition();
    static std::string hasNext(const std::string& dir, const std::string& after);
    static size_t fileSize(const std::string& dir, const std::string& name);
    void flush();

private:
    void roate();

    std::vector<std::string> listBinaryLogFiles(const std::string& after = "") const;
    static int binaryLogIdx(std::string logName);

    std::shared_ptr<std::ofstream> fptr;
    Options opts = {};
    Position position = {};
    int rotateIdx;
    static const std::string PREFIX; // "bin.log."
};
}



#endif //CHAKRA_ROTATE_BINARY_LOG_H
