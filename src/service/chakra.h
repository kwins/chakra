//
// Created by kwins on 2021/6/23.
//

#ifndef CHAKRA_CHAKRA_H
#define CHAKRA_CHAKRA_H
#include "database/db_family.h"
#include "cluster/cluster.h"
#include <ev++.h>
#include "utils/nocopy.h"
#include <list>
#include <mutex>
#include "replica/replica.h"
#include <condition_variable>
#include "net/link.h"

namespace chakra::serv{
class Chakra : public utils::UnCopyable {
public:
    struct Worker;
    struct Link : public chakra::net::Link{ // 在不同的线程中
        explicit Link(int sockfd);
        static void onPeerRead(ev::io& watcher, int event);
        void startEvRead(chakra::serv::Chakra::Worker* worker);
        ~Link();
        int workID = 0;
    };

    struct Worker{
        void startUp(int id);
        void stop();
        ~Worker();

        static void onAsync(ev::async& watcher, int event);
        static void onStopAsync(ev::async& watcher, int events);

        int workID = 0;
        ev::dynamic_loop loop{};
        ev::async async{};
        ev::async stopAsnyc{};
        std::list<Link*> links{};
        long linkNums;
    };
public:
    Chakra();
    static std::shared_ptr<Chakra> get();
    void onAccept(ev::io& watcher, int event);
    void startServCron();
    void onServCron(ev::timer& watcher, int event);
    static void onSignal(ev::sig&, int);
    void startUp();
    void stop();
    ~Chakra();

private:
    void initLibev();
    std::vector<Worker*> workers = {};
    long connNums = 0;

    ev::sig sigint;
    ev::sig sigterm;
    ev::sig sigkill;
    ev::timer cronIO;
    ev::io acceptIO;
    int sfd = -1;

    long workNum = 0;
    int successWorkers = 0;
    int exitSuccessWorkers = 0;
    std::mutex mutex = {};
    std::condition_variable cond = {};
};

}



#endif //CHAKRA_CHAKRA_H
