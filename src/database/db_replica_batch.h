#ifndef CHAKRA_DB_REPLICA_BATCH_H
#define CHAKRA_DB_REPLICA_BATCH_H
#include <cstddef>
#include <rocksdb/db.h>
#include <rocksdb/write_batch_base.h>
#include <set>
#include <unordered_map>
#include <vector>
#include "error/err.h"

namespace chakra::database {
class ReplicaBatchHandler : public rocksdb::WriteBatch::Handler {
public:
    rocksdb::WriteBatch& GetBatch();
    const std::set<std::string>& GetKeys();
    error::Error Put(const std::string& data);
    
private:
    void Put(const rocksdb::Slice &key, const rocksdb::Slice &value) override;
    void Delete(const rocksdb::Slice &key) override;
    rocksdb::WriteBatch batch;
    std::set<std::string> keys;
};

}
#endif // CHAKRA_DB_REPLICA_BATCH_H