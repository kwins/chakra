#include "command_client_get.h"
#include "client.pb.h"
#include "net/packet.h"
#include "database/db_family.h"
#include <service/chakra.h>

void chakra::cmds::CommandClientGet::execute(char *req, size_t len, void *data) {
    auto link = static_cast<chakra::serv::Chakra::Link*>(data);
    proto::client::GetMessageResponse getMessageResponse;
    proto::client::GetMessageRequest getMessageRequest;
    auto err = chakra::net::Packet::deSerialize(req, len, getMessageRequest, proto::types::C_GET);
    if (err) {
        fillError(getMessageResponse.mutable_error(), 1, err.what());
    } else {
        auto dbptr = chakra::database::FamilyDB::get();
        auto value = dbptr->get(getMessageRequest.db_name(), getMessageRequest.key());
        if (!value) {
            fillError(getMessageResponse.mutable_error(), 1, "db or key not found");
        } else {
            getMessageResponse.mutable_data()->CopyFrom(*value);
        }
    }
    link->asyncSendMsg(getMessageResponse, proto::types::C_GET);
}
