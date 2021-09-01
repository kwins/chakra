//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_DB_BLOCK_H
#define CHAKRA_DB_BLOCK_H

#include "element.h"

#include <list>
#include <unordered_map>
#include <rocksdb/db.h>
#include <mutex>
#include <shared_mutex>
#include "utils/error.h"

namespace chakra::database{

class BlockDB {
public:
    using PairIter = typename std::list<std::pair<std::string, std::shared_ptr<Element>>>::iterator;
public:
    // assimilate
    explicit BlockDB(std::shared_ptr<rocksdb::DB> dbw, std::shared_ptr<rocksdb::DB> db,size_t capacity);
    std::shared_ptr<Element> get(const std::string& key);
    void put(const std::string& key, std::shared_ptr<Element> val, bool dbput = true);
    void del(const std::string& key, bool dbdel = true);
    size_t size();
    ~BlockDB();
private:

    mutable std::mutex mutex;
    std::shared_ptr<rocksdb::DB> self = nullptr; // 当前节点只写数据，用户复制
    std::shared_ptr<rocksdb::DB> dbptr = nullptr; // 全量
    size_t capacity;
    std::list<std::pair<std::string, std::shared_ptr<Element>>> elements;
    std::unordered_map<std::string, PairIter> k2iter;
};

}



#endif //CHAKRA_DB_BLOCK_H
