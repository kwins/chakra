//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_DB_BLOCK_H
#define CHAKRA_DB_BLOCK_H

#include "element.h"

#include <list>
#include <unordered_map>
#include <mutex>
#include "utils/error.h"
#include <functional>

namespace chakra::database{

class BlockDB {
public:
    using PairIter = typename std::list<std::pair<std::string, std::shared_ptr<Element>>>::iterator;
public:
    // assimilate
    std::shared_ptr<Element> get(const std::string& key, std::function<bool(const std::string& key, std::string& val)> loadf);
    void put(const std::string& key, std::shared_ptr<Element> val);
    void del(const std::string& key);
    size_t size();
//    ~BlockDB();
private:

    mutable std::mutex mutex;
    size_t capacity;
    std::list<std::pair<std::string, std::shared_ptr<Element>>> elements;
    std::unordered_map<std::string, PairIter> k2iter;
};

}



#endif //CHAKRA_DB_BLOCK_H
