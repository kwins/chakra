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
#include "replica_link_self.h"

namespace chakra::replica{

class Replica : public utils::UnCopyable{
public:
    Replica();
    void onAccept(ev::io& watcher, int event);
    static std::shared_ptr<Replica> get();
    // self is  true ,ignore ip and port
    void setReplicateDB(const std::string& name, const std::string& ip, int port, bool self = false);
    void startReplicaCron();
    void onReplicaCron(ev::timer& watcher, int event);
    void dumpReplicaStates();
    std::list<std::shared_ptr<Link>>& getPrimaryDBLinks();
    bool replicatedPeer(const std::string& dbname, const std::string&ip, int port);
    bool replicatedSelf(const std::string& dbname);
    void stop();
private:
    void loadLinks();
    uint64_t cronLoops = 0;
    ev::io replicaio;
    ev::timer cronIO;
    int sfd = -1;
    // 复制的主节点链表
    std::list<std::shared_ptr<Link>> primaryDBLinks;
    std::list<std::shared_ptr<LinkSelf>> selfDBLinks;

    // 请求复制的连接列表
    std::list<Link*> replicaLinks;
    static const std::string REPLICA_FILE_NAME;
};

}



#endif //CHAKRA_REPLICA_H
