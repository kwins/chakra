//
// Created by kwins on 2021/6/24.
//

#include "rotate_binary_log.h"
#include <vector>
#include <dirent.h>
#include <glog/logging.h>
#include <fstream>

chakra::replica::RotateBinaryLog::RotateBinaryLog(const chakra::replica::RotateBinaryLog::Options &opts) {
    this->opts =opts;
    if (this->opts.dir[this->opts.dir.size()-1] != '/'){
        this->opts.dir += '/';
    }

    auto binLogFiles = listBinaryLogFiles();
    if (binLogFiles.empty()){
        rotateIdx = 0;
    } else {
        auto lastBinLogName = binLogFiles.back();
        auto lastIdx = binaryLogIdx(lastBinLogName);
        rotateIdx = lastIdx+ 1;
    }

    std::string filepath = opts.dir + PREFIX + std::to_string(rotateIdx);
    fptr = std::make_shared<std::ofstream>(filepath,
                                           std::ios::binary|std::ios::app|std::ios::out);
}

uint64_t chakra::replica::RotateBinaryLog::write(const char *s, size_t len) {
    auto size = fptr->tellp();
    if (size >= opts.rotateLogSize)
        roate();
    fptr->write(s, len);
    return size;
}

chakra::replica::RotateBinaryLog::Position chakra::replica::RotateBinaryLog::curPosition() {
    return Position{
        .name = PREFIX + std::to_string(rotateIdx),
        .offset = fptr->tellp()
    };
}

void chakra::replica::RotateBinaryLog::flush() { if (fptr) fptr->flush(); }

void chakra::replica::RotateBinaryLog::roate() {
    if (fptr){
        fptr->flush();
        fptr->close();
    }

    rotateIdx++;
    std::string filepath = opts.dir + PREFIX + std::to_string(rotateIdx);
    fptr = std::make_shared<std::ofstream>(filepath,
                                           std::ios::binary|std::ios::app|std::ios::out);
}

size_t chakra::replica::RotateBinaryLog::fileSize(const std::string &dir, const std::string &name) {
    std::string filepath = dir +  name;
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

std::string chakra::replica::RotateBinaryLog::hasNext(const std::string& dir, const std::string& after) {
    int idx = binaryLogIdx(after);
    if (idx == -1) return "";

    std::string nextpath = dir + PREFIX + std::to_string(idx + 1);

    std::ifstream in(nextpath, std::ios::in);
    auto good = in.good();
    in.close();

    return good ? nextpath : "";
}

std::vector<std::string> chakra::replica::RotateBinaryLog::listBinaryLogFiles(const std::string &after) const {
    DIR* dirp = opendir(opts.dir.c_str());
    dirent* ptr;
    int afterIdx = -1;
    if (!after.empty()){
        afterIdx = binaryLogIdx(after);
    }
    std::vector<std::string> binLogList;
    while ((ptr = readdir(dirp)) != nullptr){
        std::string name = ptr->d_name;
        if (ptr->d_type == DT_REG && name.find(PREFIX) != std::string::npos){
            int idx;
            if ((idx = binaryLogIdx(ptr->d_name)) != -1){
                if (afterIdx == -1 || (afterIdx >= 0 && idx >= afterIdx)){
                    binLogList.emplace_back(ptr->d_name);
                }
            }
        }
    }

    std::sort(binLogList.begin(), binLogList.end(), [](const std::string& left, const std::string& right){
        return binaryLogIdx(left) < binaryLogIdx(right);
    });

    closedir(dirp);
    return std::move(binLogList);
}

int chakra::replica::RotateBinaryLog::binaryLogIdx(std::string logName) {
    auto pos = logName.find_last_of('.');
    if (pos == std::string::npos){
        LOG(ERROR) << "Bad bin log file name " << logName;
        return -1;
    }

    auto strIdx = logName.substr(pos+1);
    return std::stoi(strIdx);
}

const std::string chakra::replica::RotateBinaryLog::PREFIX = "bin.log.";