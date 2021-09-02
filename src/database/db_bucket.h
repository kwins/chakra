//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_DB_BUCKET_H
#define CHAKRA_DB_BUCKET_H
#include <rocksdb/db.h>
#include <vector>

#include "db_block.h"
#include "utils/error.h"
#include <nlohmann/json.hpp>
#include "utils/nocopy.h"

namespace chakra::database{

class BucketDB : public utils::UnCopyable, rocksdb::WriteBatch::Handler{
public:
    struct Options{
        std::string name;
        std::string dir;
        size_t blocktSize;
        size_t blockCapaticy;
    };
public:
    explicit BucketDB(Options options);
    size_t size();
    std::shared_ptr<rocksdb::DB> getRocksDB();
    nlohmann::json dumpDB();
    std::shared_ptr<Element> get(const std::string& key);
    void put(const std::string& key, std::shared_ptr<Element> val, bool dbput = true);
    void put(rocksdb::WriteBatch &batch);
    void del(const std::string& key, bool dbdel = true);
    ~BucketDB() override;

private:
    // implement rocksdb WriteBatch Handler interface
    void Put(const rocksdb::Slice &key, const rocksdb::Slice &value) override;
    void Delete(const rocksdb::Slice &key) override;

    Options opts = {};
    std::shared_ptr<rocksdb::DB> self = nullptr; // 当前节点只写数据，用户复制
    std::shared_ptr<rocksdb::DB> dbptr = nullptr; // 全量
    std::vector<std::shared_ptr<BlockDB>> blocks;
};

}



#endif //CHAKRA_DB_BUCKET_H
