//
// Created by kwins on 2021/7/6.
//

#ifndef CHAKRA_FILE_HELPER_H
#define CHAKRA_FILE_HELPER_H

#include <fstream>
#include "utils/error.h"
#include <nlohmann/json.hpp>

namespace chakra::utils{

class FileHelper {
public:
    static size_t size(const std::string& dir, const std::string& name);
    static size_t size(const std::string& filepath);
    static utils::Error saveFile(const nlohmann::json& j, const std::string& tofile);
    static utils::Error loadFile(const std::string& fromfile, nlohmann::json& j);
};

}



#endif //CHAKRA_FILE_HELPER_H
