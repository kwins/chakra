//
// Created by kwins on 2021/6/9.
//

#include "db_block.h"
#include <glog/logging.h>
#include "type_string.h"
#include "utils/basic.h"

chakra::database::BlockDB::BlockDB(size_t capacity):capacity(capacity) {}

std::shared_ptr<chakra::database::Element> chakra::database::BlockDB::get(const std::string &key, std::function<bool(const std::string& key, std::string& value)> loadf) {
    std::lock_guard guard(mutex);
    auto it = k2iter.find(key);
    if (it == k2iter.end()){ // cache 不存在
        std::string str;
        if (!loadf(key, str))
            return nullptr;

        auto element = std::make_shared<Element>();
        element->deSeralize(str.data(), str.size());
        element->setUpdated(false);
        elements.push_front(std::make_pair(key, element));
    } else {
        // 删除元素原来在list中的位置，放到最前面
        elements.erase(it->second);
        elements.push_front(*it->second);
    }

    // 更新索引中的位置
    auto begin = elements.begin();
    k2iter[begin->first] = begin;
    return begin->second;
}

void chakra::database::BlockDB::put(const std::string &key, std::shared_ptr<Element> val) {
    std::lock_guard guard(mutex);

    auto it = k2iter.find(key);
    if (it != k2iter.end()){
        elements.erase(it->second);
        k2iter.erase(it->first);
    }
    elements.push_front(std::make_pair(key, val));
    k2iter[key] = elements.begin();

    std::string str;
    val->serialize([&str](char* data, size_t len){
        str.assign(data, len);
    });
}

void chakra::database::BlockDB::del(const std::string &key) {
    std::lock_guard guard(mutex);

    auto it = k2iter.find(key);
    if (it != k2iter.end()){
        elements.erase(it->second);
        k2iter.erase(it->first);
    }
}

size_t chakra::database::BlockDB::size() {
    std::lock_guard guard(mutex);
    size_t num = elements.size();
    return num;
}

//chakra::database::BlockDB::~BlockDB() {
//}
