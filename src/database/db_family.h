//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_DB_FAMILY_H
#define CHAKRA_DB_FAMILY_H

#include <array>
#include <element.pb.h>
#include <error/err.h>
#include <memory>
#include <replica.pb.h>
#include <rocksdb/snapshot.h>
#include <string>
#include <vector>
#include <set>
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
    
    error::Error set(const std::string& name, const std::string& key, const std::string& value, int64_t ttl = 0);
    error::Error set(const std::string& name, const std::string& key, float value, int64_t ttl = 0);

    error::Error push(const std::string& name, const std::string& key, const std::vector<std::string>& values, int64_t ttl = 0);
    error::Error push(const std::string& name, const std::string& key, const std::vector<float>& values, int64_t ttl = 0);

    error::Error incr(const std::string& name, const std::string& key, float value, int64_t ttl = 0);
    
    void erase(const std::string& name, const std::string& key);
    void cacheClear(const std::string& name);
    bool servedDB(const std::string& name);
    error::Error getMetaDB(const std::string& name,proto::peer::MetaDB& meta);
    void addDB(const std::string& name, size_t cached);
    void addDB(const std::string& name, size_t blocktSize, size_t blocktCapacity);
    void dropDB(const std::string& name);
    error::Error restoreDB(const std::string& name);
    RestoreDB getLastRestoreDB();
    std::vector<proto::replica::DBSeq> dbSeqs();
    error::Error getUpdateSince(const std::string& name, rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator>* iter);
    // 获取 db 的一个快照
    const rocksdb::Snapshot* snapshot(const std::string& name);
    void releaseSnapshot(const std::string &name, const rocksdb::Snapshot *snapshot);
    rocksdb::Iterator* iterator(const std::string &name, const rocksdb::ReadOptions& readOptions);
    error::Error getLastSeqNumber(const std::string& name, rocksdb::SequenceNumber& seq);
    size_t dbSize(const std::string& name);
    error::Error writeBatch(const std::string& name, rocksdb::WriteBatch &batch, const std::set<std::string>& keys, bool eraseCache = true);
    void stop();

private:
    using ColumnName = std::string;
    using ColumnDBs = typename std::unordered_map<ColumnName, std::shared_ptr<ColumnDB>>;
    error::Error dbnf(const std::string& str);
    std::atomic<int> index;
    std::array<ColumnDBs, 2> columnDBs;
    RestoreDB lastRestore;
    static const std::string DB_FILE;
};

}



#endif //CHAKRA_DB_FAMILY_H
