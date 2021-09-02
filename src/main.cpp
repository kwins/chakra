#include "service/chakra.h"
#include <gflags/gflags.h>

DEFINE_string(server_ip, "127.0.0.1", "server ip");                                     /* NOLINT */
DEFINE_int32(server_port, 7290, "listen port");                                         /* NOLINT */

DEFINE_string(cluster_dir, "./test/node1", "dir");                                      /* NOLINT */
DEFINE_string(cluster_ip, "127.0.0.1", "cluster ip");                                   /* NOLINT */
DEFINE_int32(cluster_port, 7291, "cluster port");                                       /* NOLINT */
DEFINE_int32(cluster_handshake_timeout_ms, 10000, "cluster handshake timeout ms");      /* NOLINT */
DEFINE_int32(cluster_peer_timeout_ms, 10000, "cluster peer timeout ms");                /* NOLINT */
DEFINE_int32(cluster_peer_link_retry_timeout_ms, 10000, "cluster peer link retry timeout ms"); /* NOLINT */
DEFINE_double(cluster_cron_interval_sec, 1.0, "cluster cron interval sec");             /* NOLINT */
DEFINE_int32(cluster_tcp_back_log, 512, "cluster tcp back log");                        /* NOLINT */


DEFINE_string(replica_ip, "127.0.0.1", "replica ip");                                   /* NOLINT */
DEFINE_int32(replica_port, 7292, "replica port");                                       /* NOLINT */
DEFINE_string(replica_dir, "./test/node1", "replica dir");                               /* NOLINT */
DEFINE_int32(replica_tcp_back_log, 512, "replica tcp back log");                        /* NOLINT */
DEFINE_int32(replica_timeout_ms, 10000, "replicas timeout ms");                         /* NOLINT */
DEFINE_int32(replica_cron_interval_sec, 1.0, "replica cron interval sec, use double");  /* NOLINT */
DEFINE_int32(replica_timeout_retry, 10, "replica timeout retry");                       /* NOLINT */

void parseOpts(chakra::serv::Chakra::Options& opts){
    opts.ip = FLAGS_server_ip;
    opts.port = FLAGS_server_port;
    opts.clusterOpts.ip = FLAGS_cluster_ip;
    opts.clusterOpts.port = FLAGS_cluster_port;
    opts.clusterOpts.dir = FLAGS_cluster_dir;
    opts.replicaOpts.ip = FLAGS_replica_ip;
    opts.replicaOpts.port = FLAGS_replica_port;
    opts.replicaOpts.dir = FLAGS_replica_dir;
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto serv = chakra::serv::Chakra::get();
    chakra::serv::Chakra::Options opts;

    parseOpts(opts);

    serv->initCharka(opts);
    serv->startUp();
    return 0;
}
