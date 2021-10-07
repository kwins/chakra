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
#include "utils/error.h"
#include <rocksdb/db.h>
#include "utils/nocopy.h"
#include <list>

namespace chakra::replica{

class Replica : public utils::UnCopyable{
public:
    Replica();
    void onAccept(ev::io& watcher, int event);
    utils::Error loadLinks();
    static std::shared_ptr<Replica> get();
    void setReplicateDB(const std::string& name, const std::string& ip, int port);
    void startReplicaCron();
    void onReplicaCron(ev::timer& watcher, int event);
    void dumpLinks();
    bool replicated(const std::string& dbname, const std::string&ip, int port);
    int replicaSuccDB(const std::string& dbname); // 复制DB状态正常的个数
    void stop();
    
private:
    uint64_t cronLoops = 0;
    ev::io replicaio;
    ev::timer cronIO;
    int sfd = -1;
    // 复制的主节点链表
    std::list<std::shared_ptr<Link>> primaryDBLinks;
    // 请求复制的连接列表
    std::list<Link*> replicaLinks;
    static const std::string REPLICA_FILE_NAME;
};

}



#endif //CHAKRA_REPLICA_H
