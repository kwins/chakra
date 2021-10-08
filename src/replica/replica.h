//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_REPLICA_H
#define CHAKRA_REPLICA_H

#include <ev++.h>
#include <string>
#include <vector>
#include <array>
#include <unordered_map>
#include "net/connect.h"
#include <google/protobuf/message.h>
#include "types.pb.h"
#include <fstream>
#include "replica_link.h"
#include <rocksdb/db.h>
#include "utils/nocopy.h"
#include <list>

namespace chakra::replica{

class Replica : public utils::UnCopyable{
public:
    Replica();
    void onAccept(ev::io& watcher, int event);
    static std::shared_ptr<Replica> get();
    void setReplicateDB(const std::string& name, const std::string& ip, int port);
    void startReplicaCron();
    void onReplicaCron(ev::timer& watcher, int event);
    void dumpReplicaStates();
    void replicaSelfDBs();
    bool replicated(const std::string& dbname, const std::string&ip, int port);
    void stop();
private:
    void loadLinks();
    uint64_t cronLoops = 0;
    ev::io replicaio;
    ev::timer cronIO;
    int sfd = -1;
    // 复制的主节点链表
    std::list<std::shared_ptr<Link>> primaryDBLinks;
    // 请求复制的连接列表
    std::list<Link*> replicaLinks;
    std::vector<proto::replica::ReplicaState> selfStates;
    static const std::string REPLICA_FILE_NAME;
};

}



#endif //CHAKRA_REPLICA_H
