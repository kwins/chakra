//
// Created by kwins on 2021/7/6.
//

#ifndef CHAKRA_FILE_HELPER_H
#define CHAKRA_FILE_HELPER_H

#include <fstream>
#include "error/err.h"
#include <nlohmann/json.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>

namespace chakra::utils{

class FileHelper {
public:
    static size_t size(const std::string& dir, const std::string& name);
    static size_t size(const std::string& filepath);
    static error::Error saveFile(const nlohmann::json& j, const std::string& tofile);
    static error::Error saveFile(const google::protobuf::Message& message, const std::string& tofile);
    static error::Error loadFile(const std::string& fromfile, nlohmann::json& j);
    static void loadFile(const std::string& fromfile, google::protobuf::Message& message);
    static error::Error mkDir(const std::string& path);
};

}



#endif //CHAKRA_FILE_HELPER_H
