//
// Created by kwins on 2021/7/6.
//

#include "file_helper.h"
#include <iostream>
#include <iterator>
#include <cstdio>
#include <unistd.h>
#include <sys/stat.h>
#include <glog/logging.h>
#include "error/err.h"

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

chakra::error::Error
chakra::utils::FileHelper::saveFile(const google::protobuf::Message &message, const std::string &tofile) {
    google::protobuf::util::JsonOptions options;
    options.always_print_enums_as_ints = true;
    options.always_print_primitive_fields = true;
    options.add_whitespace = true;
    options.preserve_proto_field_names = true;
    std::string str;
    auto s = google::protobuf::util::MessageToJsonString(message, &str, options);
    if (!s.ok())
        return error::Error(s.ToString());

    std::string tmpfile = tofile + ".tmp";
    std::ofstream out(tmpfile, std::ios::out|std::ios::trunc);
    if (!out.is_open()){
        if (errno == ENOENT){
            return error::Error(tofile + " not exist");
        }
        return error::Error(strerror(errno));
    }
    out << str;
    out.close();
    if (::rename(tmpfile.c_str(), tofile.c_str()) == -1){
        return error::FileError(strerror(errno));
    }
    return error::Error();
}

void chakra::utils::FileHelper::loadFile(const std::string &fromfile, google::protobuf::Message &message) {
    std::ifstream fileStream(fromfile);
    if (!fileStream.is_open()){
        if (errno == ENOENT){
            throw error::FileError(fromfile + " not exist");
        }
        throw error::Error(strerror(errno));
    }

    std::istream_iterator<char> begin(fileStream), end;
    std::string str(begin, end);
    fileStream.close();
    auto s = google::protobuf::util::JsonStringToMessage(str, &message);
    if (!s.ok())
        throw error::Error(s.ToString());
}

chakra::error::Error chakra::utils::FileHelper::mkdir(const std::string &path) {
    std::string str = path;
    std::string str1;
    int pos = 0;
    while (pos >= 0) {
        pos = str.find('/');
        str1 += str.substr(0, pos) + "/";
        if (::access(str1.c_str(), 0) != 0) {
            int ret = ::mkdir(str1.c_str(), S_IRWXU);
            if (ret != 0) {
                return error::Error(strerror(errno));
            }
        }
        str = str.substr(pos + 1, str.size());
    }
    return error::Error();
}
