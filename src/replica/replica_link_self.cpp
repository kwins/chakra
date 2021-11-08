//
// Created by kwins on 2021/10/13.
//

#include "replica_link_self.h"
#include "database/db_family.h"
#include <glog/logging.h>
#include "utils/basic.h"
DECLARE_double(replica_delta_pull_interval_sec);
DECLARE_int64(replica_delta_batch_bytes);
DECLARE_double(replica_bulk_send_interval_sec);
DECLARE_int64(replica_bulk_batch_bytes);

chakra::replica::LinkSelf::LinkSelf(std::string dbname)
: dbName(std::move(dbname)),deltaSeq(-1), startMs(0), bulkCnt(0){
    prepareReplica();
}

chakra::replica::LinkSelf::LinkSelf(std::string  dbname, int64_t seq)
: dbName(std::move(dbname)),deltaSeq(seq),startMs(0), bulkCnt(0) {
    prepareReplica();
}

void chakra::replica::LinkSelf::prepareReplica() {
    if (deltaSeq < 0){
        LOG(INFO) << "replica self " << getDbName()
                  << " delta seq " << deltaSeq
                  << " start snapshot bulk.";
        snapshotBulk();
        return;
    }

    auto& db = chakra::database::FamilyDB::get();
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto err = db.getUpdateSince(dbName, deltaSeq, &iter);
    if (!err.success()){
        LOG(ERROR) << err.what();
        exit(-1);
    }

    rocksdb::SequenceNumber lastSeq;
    db.getLastSeqNumber(getDbName(), lastSeq);
    if ((iter->Valid() && iter->GetBatch().sequence == deltaSeq)
        || (!iter->Valid() && lastSeq == deltaSeq)){
        LOG(INFO) << "replica self " << getDbName()
                  << " delta seq " << deltaSeq
                  << " last seq " << lastSeq
                  << " start pull delta.";
        startPullDelta();
    } else {
        LOG(INFO) << "replica self " << getDbName()
                  << " delta seq " << deltaSeq
                  << " last seq " << lastSeq
                  << " start snapshot bulk.";
        snapshotBulk();
    }
}

chakra::error::Error chakra::replica::LinkSelf::snapshotBulk() {
    auto& dbptr = chakra::database::FamilyDB::get();
    rocksdb::SequenceNumber lastSeq;
    auto err = dbptr.snapshot(getDbName(), &bulkiter, lastSeq);
    if (!err.success()){
        LOG(ERROR) << "replica self create db " << getDbName() << " snapshot error " << err.what();
    } else {
        LOG(INFO) << "replica start send self " << getDbName()
                  << " bulk seq " << deltaSeq;
        deltaSeq = lastSeq;
        bulkiter->SeekToFirst();
        startMs = utils::Basic::getNowMillSec();
        startSendBulk();
    }
    return err;
}

void chakra::replica::LinkSelf::startSendBulk() {
    transferIO.set<chakra::replica::LinkSelf, &chakra::replica::LinkSelf::onSendBulk>(this);
    transferIO.set(ev::get_default_loop());
    transferIO.start(FLAGS_replica_bulk_send_interval_sec);
}

void chakra::replica::LinkSelf::onSendBulk(ev::timer &watcher, int event) {
    auto& dbptr = chakra::database::FamilyDB::get();
    int size = 0;
    while (bulkiter->Valid()){
        size += bulkiter->key().size();
        size += bulkiter->value().size();
        if (size > FLAGS_replica_bulk_batch_bytes)
            break;
        auto err = dbptr.putAll(getDbName(), bulkiter->key(), bulkiter->value());
        if (!err.success()){
            LOG(ERROR) << err.what();
        }
        bulkiter->Next();
        bulkCnt++;
    }

    if (bulkiter->Valid()){ // 继续传输
        startSendBulk();
    } else { // 全量发送完成
        LOG(INFO) << "replica send self " << getDbName()
                  << " bulk finished, send "
                  << bulkCnt << " keys, spends "
                  << (utils::Basic::getNowMillSec() - startMs)
                  << " ms.";
        transferIO.stop();
        delete bulkiter;
        bulkiter = nullptr;
        bulkCnt = 0;
        startPullDelta();
    }
}

void chakra::replica::LinkSelf::startPullDelta() {
    deltaIO.set<chakra::replica::LinkSelf, &chakra::replica::LinkSelf::onPullDelta>(this);
    deltaIO.set(ev::get_default_loop());
    deltaIO.start(FLAGS_replica_delta_pull_interval_sec);
}

void chakra::replica::LinkSelf::onPullDelta(ev::timer &watcher, int event) {
    auto&db = database::FamilyDB::get();
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto err = db.getUpdateSince(dbName, deltaSeq, &iter);
    if (!err.success()){
        LOG(ERROR) << "replica self fetch update error " << err.what();
    } else {
        int bytes = 0;
        while (iter->Valid()){
            auto batch = iter->GetBatch();
            if (bytes >= FLAGS_replica_delta_batch_bytes) break;
            if (batch.sequence > deltaSeq){
                bytes += batch.writeBatchPtr->Data().size();
                err = db.putAll(dbName, *batch.writeBatchPtr);
                if (err.success()){
                    deltaSeq = batch.sequence;
                } else {
                    LOG(ERROR) << "replica self db put all error " << err.what();
                }
            }
            iter->Next();
        }
    }
    startPullDelta();
}

const std::string &chakra::replica::LinkSelf::getDbName() const { return dbName; }

void chakra::replica::LinkSelf::setDbName(const std::string &dbname) { dbName = dbname; }

int64_t chakra::replica::LinkSelf::getDeltaSeq() const { return deltaSeq; }

void chakra::replica::LinkSelf::setDeltaSeq(int64_t seq) {deltaSeq = seq; }
