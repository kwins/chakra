//
// Created by kwins on 2021/7/6.
//

#ifndef CHAKRA_FILE_HELPER_H
#define CHAKRA_FILE_HELPER_H

#include <fstream>
#include "error/err.h"
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>

namespace chakra::utils{

class FileHelper {
public:
    static size_t size(const std::string& dir, const std::string& name);
    static size_t size(const std::string& filepath);
    static error::Error saveFile(const google::protobuf::Message& message, const std::string& tofile);
    static void loadFile(const std::string& fromfile, google::protobuf::Message& message);
    static error::Error mkdir(const std::string& path);
};

}



#endif //CHAKRA_FILE_HELPER_H
