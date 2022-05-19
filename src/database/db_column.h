//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_DB_BUCKET_H
#define CHAKRA_DB_BUCKET_H
#include <memory>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <vector>
#include "error/err.h"
#include "utils/nocopy.h"
#include "peer.pb.h"
#include "element.h"
#include "db_column_cache.h"

namespace chakra::database {

class ColumnDB : public rocksdb::WriteBatch::Handler {
public:
    struct RestoreDB {
        rocksdb::SequenceNumber seq;
        std::string filepath;
    };
public:
    explicit ColumnDB(const proto::peer::MetaDB& meta);
    size_t size();
    std::shared_ptr<proto::element::Element> get(const std::string& key);

    void set(const std::string& key, const std::string& value, int64_t ttl = 0);
    void set(const std::string& key, int64_t value, int64_t ttl = 0);
    void set(const std::string& key, float value, int64_t ttl = 0);

    void push(const std::string& key, const std::vector<std::string>& values, int64_t ttl = 0);
    void push(const std::string& key, const std::vector<int64_t>& values, int64_t ttl = 0);
    void push(const std::string& key, const std::vector<float>& values, int64_t ttl = 0);

    error::Error incr(const std::string& key, int64_t value, int64_t ttl = 0);
    error::Error incr(const std::string& key, float value, int64_t ttl = 0);

    void erase(const std::string& key);
    float cacheUsage();

    // void put(const std::string& key, std::shared_ptr<Element> val, bool dbput = true);
    // 增量同步过来的数据，需要写入到全量的rocksdb中，并清除缓存
    error::Error rocksWriteBulk(rocksdb::WriteBatch &batch);
    // error::Error putAll(const std::string& key, const std::string& value);
    // error::Error putAll(const rocksdb::Slice& key, const rocksdb::Slice& value);
    // void del(const std::string& key, bool dbdel = true);

    proto::peer::MetaDB getMetaDB(const std::string& dbname);
    error::Error restoreDB();
    RestoreDB getLastRestoreDB();
    error::Error getUpdateSince(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator>* iter);
    // 获取 db 的一个快照 和 快照对应增量 seq
    error::Error snapshot(rocksdb::Iterator** iter, rocksdb::SequenceNumber& seq);
    rocksdb::SequenceNumber getLastSeqNumber();

    ~ColumnDB() override;

private:
    // implement rocksdb WriteBatch Handler interface
    void Put(const rocksdb::Slice &key, const rocksdb::Slice &value) override;
    void Delete(const rocksdb::Slice &key) override;

    proto::peer::MetaDB metaDB;
    RestoreDB lastRestore;
    std::shared_ptr<rocksdb::DB> self = nullptr;                      // 节点写增量
    std::shared_ptr<rocksdb::DB> full = nullptr;                     // 全量
    // std::vector<std::shared_ptr<Cache>> caches;
    std::vector<std::shared_ptr<ColumnDBLRUCache>>  cache;
};

}



#endif //CHAKRA_DB_BUCKET_H
