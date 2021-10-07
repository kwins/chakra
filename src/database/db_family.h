//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_DB_FAMILY_H
#define CHAKRA_DB_FAMILY_H

#include <array>
#include <vector>
#include <unordered_map>
#include "db_bucket.h"
#include "utils/nocopy.h"
#include "utils/error.h"

namespace chakra::database{

class FamilyDB : public utils::UnCopyable{
public:
    struct RestoreDB{
        rocksdb::SequenceNumber seq;
        std::string filepath;
    };
public:
    FamilyDB();
    static FamilyDB& get();

    bool servedDB(const std::string& name);
    utils::Error getMetaDB(const std::string& dbname,proto::peer::MetaDB& meta);
    void addDB(const std::string& name, size_t cached);
    void addDB(const std::string& name, size_t blocktSize, size_t blocktCapacity);
    void dropDB(const std::string& name);
    utils::Error restoreDB(const std::string& name);
    RestoreDB getLastRestoreDB();
    utils::Error getUpdateSince(const std::string& name, rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator>* iter);
    // 获取 db 的一个快照 和 快照对应增量 seq
    utils::Error snapshot(const std::string& name, rocksdb::Iterator** iter, rocksdb::SequenceNumber& seq);
    utils::Error getLastSeqNumber(const std::string& name, rocksdb::SequenceNumber& seq);
    size_t dbSize(const std::string& name);

    // 会先从内存中获取，获取不到则尝试从全量rocksDB拿
    std::shared_ptr<Element> get(const std::string& name, const std::string& key);

    // 只用于实时写和删除，只会写或者删除内存数据
    void put(const std::string& name, const std::string& key, std::shared_ptr<Element> val);
    void del(const std::string& name, const std::string& key);

    // 写全量rocksDB
    utils::Error put(const std::string& name, rocksdb::WriteBatch& batch);
    ~FamilyDB();
private:
    using ColumnName = std::string;
    using ColumnBuckets = typename std::unordered_map<ColumnName, std::shared_ptr<BucketDB>>;

    std::atomic<int> index = 0;
    std::array<ColumnBuckets, 2> columnBuckets;
    RestoreDB lastRestore;
    static const std::string DB_FILE;
};

}



#endif //CHAKRA_DB_FAMILY_H
