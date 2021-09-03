//
// Created by kwins on 2021/7/6.
//

#include "file_helper.h"
#include <fstream>

size_t chakra::utils::FileHelper::size(const std::string &dir, const std::string &name) {
    std::string filepath;
    if (dir[dir.size()-1] == '/'){
        filepath = dir +  name;
    } else {
        filepath = dir + "/" + name;
    }
    return size(filepath);
}

size_t chakra::utils::FileHelper::size(const std::string &filepath) {
    std::ifstream in(filepath, std::ios::in);
    auto good = in.good();
    size_t size = 0;
    if (good){
        in.seekg(0, std::ios::end);
        size = in.tellg();
    }
    in.close();
    return size;
}

chakra::utils::Error chakra::utils::FileHelper::saveFile(const nlohmann::json &j, const std::string& tofile) {
    std::string tmpfile = tofile + ".tmp";
    std::ofstream out(tmpfile, std::ios::out|std::ios::trunc);
    if (!out.is_open()){
        if (errno == ENOENT){
            return utils::Error(utils::Error::ERR_FILE_NOT_EXIST,  "file: " + tofile + " not exist");
        }
        return utils::Error(utils::Error::ERR_ERRNO, strerror(errno));
    }
    out << j.dump(4);
    if (::rename(tmpfile.c_str(), tofile.c_str()) == -1){
        return utils::Error(utils::Error::ERR_FILE_RENAME, strerror(errno));
    }
    out.close();
    return utils::Error();
}

chakra::utils::Error chakra::utils::FileHelper::loadFile(const std::string &fromfile, nlohmann::json &j) {
    std::ifstream fileStream(fromfile);
    if (!fileStream.is_open()){
        if (errno == ENOENT){
            return utils::Error(utils::Error::ERR_FILE_NOT_EXIST,  "file: " + fromfile + " not exist");
        }
        return utils::Error(utils::Error::ERR_ERRNO, strerror(errno));
    }
    fileStream >> j;
    fileStream.close();
    return utils::Error();
}
