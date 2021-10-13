//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_DB_BUCKET_H
#define CHAKRA_DB_BUCKET_H
#include <rocksdb/db.h>
#include <vector>
#include "db_block.h"
#include "error/err.h"
#include <nlohmann/json.hpp>
#include "utils/nocopy.h"
#include "peer.pb.h"

namespace chakra::database{

class BucketDB :public rocksdb::WriteBatch::Handler{
public:
    struct RestoreDB{
        rocksdb::SequenceNumber seq;
        std::string filepath;
    };
public:
    explicit BucketDB(const proto::peer::MetaDB& meta);
    size_t size();
    std::shared_ptr<Element> get(const std::string& key);
    void put(const std::string& key, std::shared_ptr<Element> val, bool dbput = true);
    error::Error putAll(rocksdb::WriteBatch &batch);
    error::Error putAll(const std::string& key, const std::string& value);
    error::Error putAll(const rocksdb::Slice& key, const rocksdb::Slice& value);
    void del(const std::string& key, bool dbdel = true);

    proto::peer::MetaDB getMetaDB(const std::string& dbname);
    error::Error restoreDB();
    RestoreDB getLastRestoreDB();
    error::Error getUpdateSince(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator>* iter);
    // 获取 db 的一个快照 和 快照对应增量 seq
    error::Error snapshot(rocksdb::Iterator** iter, rocksdb::SequenceNumber& seq);
    rocksdb::SequenceNumber getLastSeqNumber();

    ~BucketDB() override;

private:
    // implement rocksdb WriteBatch Handler interface
    void Put(const rocksdb::Slice &key, const rocksdb::Slice &value) override;
    void Delete(const rocksdb::Slice &key) override;

//    Options opts = {};
    proto::peer::MetaDB metaDB;
    RestoreDB lastRestore;
    std::shared_ptr<rocksdb::DB> self = nullptr;                      // 节点写增量
    std::shared_ptr<rocksdb::DB> dbptr = nullptr;                     // 全量
    std::vector<std::shared_ptr<BlockDB>> blocks;
};

}



#endif //CHAKRA_DB_BUCKET_H
