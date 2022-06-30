//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_DB_BUCKET_H
#define CHAKRA_DB_BUCKET_H
#include <memory>
#include <replica.pb.h>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <vector>
#include <set>
#include "error/err.h"
#include "utils/nocopy.h"
#include "peer.pb.h"
#include "db_column_cache.h"

namespace chakra::database {

class ColumnDB {
public:
    struct RestoreDB {
        rocksdb::SequenceNumber seq;
        std::string filepath;
    };
public:
    explicit ColumnDB(const proto::peer::MetaDB& meta);
    size_t size();
    std::shared_ptr<proto::element::Element> get(const std::string& key);
    std::vector<std::shared_ptr<proto::element::Element>> mget(const std::vector<std::string>& keys);

    error::Error set(const std::string& key, const std::string& value, int64_t ttl = 0);
    error::Error set(const std::string& key, float value, int64_t ttl = 0);

    error::Error push(const std::string& key, const std::vector<std::string>& values, int64_t ttl = 0);
    error::Error push(const std::string& key, const std::vector<float>& values, int64_t ttl = 0);

    error::Error incr(const std::string& key, float value, int64_t ttl = 0);

    void erase(const std::string& key);
    float cacheUsage();
    void cacheClear();
    
    // 增量或者全量数据同步时采用 batch write
    error::Error writeBatch(rocksdb::WriteBatch &batch, std::set<std::string> batchKeys);
    proto::peer::MetaDB getMetaDB(const std::string& dbname);
    error::Error restoreDB();
    RestoreDB getLastRestoreDB();
    error::Error getUpdateSince(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator>* iter);
    // 获取 DB 的一个快照
    const rocksdb::Snapshot* snapshot();
    void releaseSnapshot(const rocksdb::Snapshot *snapshot);
    // 获取 DB 快照的迭代器
    rocksdb::Iterator* iterator(const rocksdb::ReadOptions& readOptions);
    rocksdb::SequenceNumber getLastSeqNumber();

    ~ColumnDB();

private:
    proto::peer::MetaDB metaDB;
    RestoreDB lastRestore;
    std::shared_ptr<rocksdb::DB> self = nullptr;                      // 节点写增量
    std::shared_ptr<rocksdb::DB> full = nullptr;                     // 全量
    std::vector<std::shared_ptr<ColumnDBLRUCache>>  caches;
};

}



#endif //CHAKRA_DB_BUCKET_H
