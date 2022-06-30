#ifndef CHAKRA_DB_REPLICA_BATCH_H
#define CHAKRA_DB_REPLICA_BATCH_H
#include <rocksdb/db.h>
#include <rocksdb/write_batch_base.h>
#include <vector>
#include "error/err.h"

namespace chakra::database {
class ReplicaBatchHandler : public rocksdb::WriteBatch::Handler {
public:
    rocksdb::WriteBatch& GetBatch();
    std::vector<std::string>& GetKeys();
    error::Error Put(const std::string& data);
    
private:
    void Put(const rocksdb::Slice &key, const rocksdb::Slice &value) override;
    void Delete(const rocksdb::Slice &key) override;
    rocksdb::WriteBatch batch;
    std::vector<std::string> keys;
};

}
#endif // CHAKRA_DB_REPLICA_BATCH_H