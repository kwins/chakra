//
// Created by kwins on 2021/6/23.
//

#ifndef CHAKRA_CHAKRA_H
#define CHAKRA_CHAKRA_H
#include "cluster/view.h"
#include <ev++.h>
#include "utils/nocopy.h"
#include <list>
#include <mutex>
#include "replica/replica.h"
#include <condition_variable>

namespace chakra::serv{
class Chakra : public utils::UnCopyable {
public:
    struct Options{
        // server
        int port = 7290;
        int tcpBackLog = 512;
        std::string ip = "127.0.0.1";
        float cronInterval = 1;
        chakra::cluster::View::Options clusterOpts;
        chakra::replica::Replica::Options replicaOpts;
    };

    struct Worker;
    struct Link{ // 在不同的线程中
        explicit Link(int sockfd);
        static void onPeerRead(ev::io& watcher, int event);
        void startEvRead(chakra::serv::Chakra::Worker* worker);
        void close() const;
        ~Link();

        std::shared_ptr<net::Connect> conn;
        std::shared_ptr<ev::io> rio;
        int workID;
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
    static std::shared_ptr<Chakra> get();
    void initCharka(const Options& opts);
    void onAccept(ev::io& watcher, int event);
    void startServCron();
    void onServCron(ev::timer& watcher, int event);
    static void onSignal(ev::sig&, int);
    void startUp();
    void stop();
    ~Chakra();

private:
    void initLibev();
    Options opts = {};
    std::vector<Worker*> workers = {};
    long connNums = 0;

    ev::sig sigint;
    ev::sig sigterm;
    ev::sig sigkill;
    ev::timer cronIO;
    ev::io acceptIO;
    int sfd;

    long workNum = 0;
    int successWorkers = 0;
    int exitSuccessWorkers = 0;
    std::mutex mutex = {};
    std::condition_variable cond = {};
};

}



#endif //CHAKRA_CHAKRA_H
