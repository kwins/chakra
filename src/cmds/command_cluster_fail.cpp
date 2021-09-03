//
// Created by kwins on 2021/6/3.
//

#include "command_cluster_fail.h"
#include "cluster/view.h"
#include "peer.pb.h"
#include "net/packet.h"
#include "cluster/peer.h"
#include "utils/basic.h"

void chakra::cmds::CommandClusterFail::execute(char *req, size_t len, void *data, std::function<utils::Error(char *, size_t)> reply) {
    proto::peer::FailMessage failMessage;
    if (!chakra::net::Packet::deSerialize(req, len, failMessage, proto::types::P_FAIL).success()) return;

    uint64_t todo = 0;
    std::shared_ptr<cluster::Peer> sender = cluster::View::get()->getPeer(failMessage.sender().name());
    if (sender && !sender->isHandShake()){
        // 更新纪元
        if (sender->getEpoch() < failMessage.sender().config_epoch())
            sender->setEpoch(failMessage.sender().config_epoch());

        if (cluster::View::get()->getCurrentEpoch() < failMessage.sender().current_epoch()){
            cluster::View::get()->setCurrentEpoch(failMessage.sender().current_epoch());
            todo |= (cluster::View::FLAG_SAVE_CONFIG | cluster::View::FLAG_UPDATE_STATE);
        }
    }

    if (sender){
       auto failing = cluster::View::get()->getPeer(failMessage.fail_peer_name());
       if (failing && !failing->isFail() && !failing->isMyself()){
           LOG(WARNING) << "FAIL message received from " << failMessage.sender().name() << " about " << failMessage.fail_peer_name();
           failing->setFlag(cluster::Peer::FLAG_FAIL);
           failing->setFailTime(utils::Basic::getNowMillSec());
           failing->delFlag(cluster::Peer::FLAG_PFAIL);
           cluster::View::get()->setCronTODO(cluster::View::FLAG_SAVE_CONFIG|cluster::View::FLAG_UPDATE_STATE);
       }
    } else {
        LOG(WARNING) << "Ignoring FAIL message from unknonw peer " << failMessage.sender().name() << " about " << failMessage.fail_peer_name();
    }

    if (todo) cluster::View::get()->setCronTODO(todo);
}

