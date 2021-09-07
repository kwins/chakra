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

class BucketDB :public rocksdb::WriteBatch::Handler{
public:
    struct Options{
        std::string name;
        std::string dir;
        size_t blocktSize;
        size_t blockCapaticy;
        long dbWALTTLSeconds = 86400;
    };

    struct RestoreDB{
        rocksdb::SequenceNumber seq;
        std::string filepath;
    };
public:
    explicit BucketDB(Options options);
    size_t size();
    nlohmann::json dumpDB();
    std::shared_ptr<Element> get(const std::string& key);
    void put(const std::string& key, std::shared_ptr<Element> val, bool dbput = true);
    void put(rocksdb::WriteBatch &batch);
    void del(const std::string& key, bool dbdel = true);

    utils::Error restoreDB();
    RestoreDB getLastRestoreDB();
    utils::Error fetch(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator>* iter);
    // 获取 db 的一个快照 和 快照对应增量 seq
    utils::Error snapshot(rocksdb::Iterator** iter, rocksdb::SequenceNumber& seq);
    rocksdb::SequenceNumber getLastSeqNumber();

    ~BucketDB() override;

private:
    // implement rocksdb WriteBatch Handler interface
    void Put(const rocksdb::Slice &key, const rocksdb::Slice &value) override;
    void Delete(const rocksdb::Slice &key) override;

    Options opts = {};
    RestoreDB lastRestore;
    rocksdb::DB* self;                      // 节点写增量
    rocksdb::DB* dbptr;                     // 全量
    std::vector<std::shared_ptr<BlockDB>> blocks;
};

}



#endif //CHAKRA_DB_BUCKET_H
