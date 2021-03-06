#include "db_replica_batch.h"
#include <error/err.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rocksdb/write_batch_base.h>
#include <zlib.h>

DECLARE_int32(db_cache_shard_size);

namespace chakra::database {

rocksdb::WriteBatch& ReplicaBatchHandler::GetBatch() { return batch; }
const std::set<std::string>& ReplicaBatchHandler::GetKeys() { return keys; }

error::Error ReplicaBatchHandler::Put(const std::string& data) {
    rocksdb::WriteBatch sub(data);
    auto s = sub.Iterate(this);
    if (!s.ok()) return error::Error(s.ToString());
    return error::Error();
}

void ReplicaBatchHandler::Put(const rocksdb::Slice &key, const rocksdb::Slice &value) {
    auto s = batch.Put(key, value);
    if (!s.ok()) LOG(ERROR) << s.ToString();
    else keys.emplace(key.ToString());
}

void ReplicaBatchHandler::Delete(const rocksdb::Slice &key) {
    auto s = batch.Delete(key);
    if (!s.ok()) LOG(ERROR) << s.ToString();
    else keys.emplace(key.ToString());
}

}