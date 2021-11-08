//
// Created by kwins on 2021/10/13.
//

#ifndef CHAKRA_REPLICA_LINK_SELF_H
#define CHAKRA_REPLICA_LINK_SELF_H
#include <rocksdb/db.h>
#include <ev++.h>
#include "error/err.h"

namespace chakra::replica {
class LinkSelf {
public:
    explicit LinkSelf(std::string  dbname);
    explicit LinkSelf(std::string  dbname, int64_t seq);

    // 创建db snapshot, 为后续传输 db 做准备
    error::Error snapshotBulk();
    // 触发全量复制
    void startSendBulk();
    // 执行全量复制
    void onSendBulk(ev::timer& watcher, int event);
    // 触发增量拉取
    void startPullDelta();
    void onPullDelta(ev::timer& watcher, int event);

    const std::string &getDbName() const;
    void setDbName(const std::string &dbname);
    int64_t getDeltaSeq() const;
    void setDeltaSeq(int64_t seq);

private:
    void prepareReplica();
    std::string dbName;
    rocksdb::Iterator* bulkiter = nullptr;
    long startMs;
    long bulkCnt;
    int64_t deltaSeq = -1;
    ev::timer transferIO;
    ev::timer deltaIO;
};

}

#endif //CHAKRA_REPLICA_LINK_SELF_H
