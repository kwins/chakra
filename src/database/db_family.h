//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_DB_FAMILY_H
#define CHAKRA_DB_FAMILY_H

#include <array>
#include <element.pb.h>
#include <error/err.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include "db_column.h"
#include "utils/nocopy.h"

namespace chakra::database {

class FamilyDB : public utils::UnCopyable {
public:
    struct RestoreDB {
        rocksdb::SequenceNumber seq;
        std::string filepath;
    };
public:
    FamilyDB();
    static std::shared_ptr<chakra::database::FamilyDB>& get();

    // 会先从内存中获取，获取不到则尝试从全量rocksDB拿
    std::shared_ptr<proto::element::Element> get(const std::string& name, const std::string& key);
    std::unordered_map<std::string, std::vector<std::shared_ptr<proto::element::Element>>> 
    mget(std::unordered_map<std::string, std::vector<std::string>> dbkeys);
    
    void set(proto::element::Element&& element);
    void set(const std::string& name, const std::string& key, const std::string& value, int64_t ttl = 0);
    void set(const std::string& name, const std::string& key, float value, int64_t ttl = 0);

    void push(const std::string& name, const std::string& key, const std::vector<std::string>& values, int64_t ttl = 0);
    void push(const std::string& name, const std::string& key, const std::vector<float>& values, int64_t ttl = 0);

    error::Error incr(const std::string& name, const std::string& key, float value, int64_t ttl = 0);

    void erase(const std::string& name, const std::string& key);

    bool servedDB(const std::string& name);
    error::Error getMetaDB(const std::string& dbname,proto::peer::MetaDB& meta);
    void addDB(const std::string& name, size_t cached);
    void addDB(const std::string& name, size_t blocktSize, size_t blocktCapacity);
    void dropDB(const std::string& name);
    error::Error restoreDB(const std::string& name);
    RestoreDB getLastRestoreDB();
    error::Error getUpdateSince(const std::string& name, rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator>* iter);
    // 获取 db 的一个快照 和 快照对应增量 seq
    error::Error snapshot(const std::string& name, rocksdb::Iterator** iter, rocksdb::SequenceNumber& seq);
    error::Error getLastSeqNumber(const std::string& name, rocksdb::SequenceNumber& seq);
    size_t dbSize(const std::string& name);

    // 同步时写入全量 RocksDB 中
    error::Error rocksWriteBulk(const std::string& name, rocksdb::WriteBatch& batch);
    void stop();

private:
    using ColumnName = std::string;
    using ColumnDBs = typename std::unordered_map<ColumnName, std::shared_ptr<ColumnDB>>;

    std::atomic<int> index;
    std::array<ColumnDBs, 2> columnDBs;
    RestoreDB lastRestore;
    static const std::string DB_FILE;
};

}



#endif //CHAKRA_DB_FAMILY_H
