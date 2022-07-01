//
// Created by kwins on 2021/9/30.
//

#ifndef CHAKRA_FLAGS_CPP
#define CHAKRA_FLAGS_CPP

#include <cstdint>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <thread>
#include "utils/file_helper.h"

DEFINE_int32(server_port, 7290, "chakra server listen port");                                       /* NOLINT */
DEFINE_int32(server_tcp_backlog, 512, "chakra server tcp back log");                                /* NOLINT */
DEFINE_int32(server_workers, std::thread::hardware_concurrency(), "chakra server workers");   
static bool validServerWorkers(const char* flagname, int32_t value) {
    if (value <= 0) return false;
    LOG(INFO) << "The number of hardware concurrency configured is " <<  std::thread::hardware_concurrency();
    LOG(INFO) << "The number of processors configured is " <<  sysconf(_SC_NPROCESSORS_CONF);
    LOG(INFO) << "The number of processors currently online (available) is " << sysconf(_SC_NPROCESSORS_ONLN);
    LOG(INFO) << "The pagesize: " << sysconf(_SC_PAGESIZE);
    LOG(INFO) << "The number of pages: " << sysconf(_SC_PHYS_PAGES);  
    LOG(INFO) << "The number of available pages: " << sysconf(_SC_AVPHYS_PAGES); 
    LOG(INFO) << "The memory size: " <<  (long long)sysconf(_SC_PAGESIZE) * (long long)sysconf(_SC_PHYS_PAGES) / (1024 * 1024) << " MB";
    LOG(INFO) << "The number of files max opened:: " << sysconf(_SC_OPEN_MAX);
    LOG(INFO) << "The number of ticks per second: " << sysconf(_SC_CLK_TCK);  
    LOG(INFO) << "The max length of host name: " << sysconf(_SC_HOST_NAME_MAX); 
    LOG(INFO) << "The max length of login name: " << sysconf(_SC_LOGIN_NAME_MAX); 
    return true;
}
DEFINE_validator(server_workers, validServerWorkers);    
DEFINE_double(server_cron_interval_sec, 1.0, "chakra server cron interval sec");                    /* NOLINT */


DEFINE_string(replica_dir, "data", "replica dir");                                                  /* NOLINT */
static bool validReplicaDir(const char* flagname, const std::string& value) {
    if (value.empty()) return false;
    auto err = chakra::utils::FileHelper::mkdir(value);
    if (err){
        LOG(ERROR) << "valid replica dir error " << err.what();
        return false;
    }
    return true;
}
DEFINE_validator(replica_dir, validReplicaDir);                                                                         /* NOLINT */
DEFINE_int32(replica_timeout_ms, 15000, "replicas timeout ms");                                                         /* NOLINT */
DEFINE_double(replica_cron_interval_sec, 0.5, "replica cron interval sec, use double");                                 /* NOLINT */
DEFINE_int32(replica_timeout_retry, 10, "replica timeout retry");                                                       /* NOLINT */
DEFINE_double(replica_delta_pull_interval_sec, 0.1, "replica pull db dalta interval sec");                              /* NOLINT */
DEFINE_int64(replica_delta_batch_bytes, 1024 * 1024 * 3, "replica delta batch size");                                   /* NOLINT */
DEFINE_int64(replica_delta_delay_num, 10000, "replica delta delay num");                                                /* NOLINT */
DEFINE_double(replica_bulk_send_interval_sec, 0.1, "replica send db bulk interval sec");                                /* NOLINT */
DEFINE_int64(replica_bulk_batch_bytes, 1024 * 1024 * 3, "replica bulk batch size");                                     /* NOLINT */

DEFINE_string(db_dir, "data", "rocksdb save dir");                                                                      /* NOLINT */
static bool validDbDir(const char* flagname, const std::string& value) {
    if (value.empty()) return false;
    auto err = chakra::utils::FileHelper::mkdir(value);
    if (err) return false;
    return true;
}
DEFINE_validator(db_dir, validDbDir);                                                                                   /* NOLINT */
DEFINE_string(db_restore_dir, "data", "rocksdb restore dir");                                                           /* NOLINT */
DEFINE_string(db_backup_dir, "data", "rocksdb backup dir");                                                             /* NOLINT */
DEFINE_int32(db_cache_shard_size, std::thread::hardware_concurrency() * 2, "rocksdb cached shard size");                /* NOLINT */
DEFINE_int64(db_default_cache_bytes, 1024 * 1024 * 200, "rocksdb cached default cache size default 200MB");             /* NOLINT */
DEFINE_int64(db_wal_ttl_seconds, 86400 * 5, "rocksdb wal log ttl seconds default 5 days");                              /* NOLINT */


DEFINE_string(cluster_dir, "data", "cluster dir");                                              /* NOLINT */
static bool validClusterDir(const char* flagname, const std::string& value) {
    if (value.empty()) return false;
    auto err = chakra::utils::FileHelper::mkdir(value);
    if (err) return false;
    return true;
}
DEFINE_validator(cluster_dir, validClusterDir);                                                 /* NOLINT */
DEFINE_int32(cluster_handshake_timeout_ms, 15000, "cluster handshake timeout ms");              /* NOLINT */
DEFINE_int32(cluster_peer_timeout_ms, 15000, "cluster peer timeout ms");                        /* NOLINT */
DEFINE_int32(cluster_peer_link_retry_timeout_ms, 15000, "cluster peer link retry timeout ms");  /* NOLINT */
DEFINE_double(cluster_cron_interval_sec, 0.5, "cluster cron interval sec");                     /* NOLINT */

#endif //CHAKRA_FLAGS_CPP
