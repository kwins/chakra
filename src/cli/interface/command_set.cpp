#include "command_set.h"
#include <client.pb.h>

DECLARE_string(set_db);
DECLARE_string(set_keys);
DECLARE_string(set_values);
DECLARE_int64(set_ttl); // ms

void chakra::cli::CommandSet::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    auto dbkeys = stringSplit(FLAGS_set_keys, ',');
    auto values = stringSplit(FLAGS_set_values, ',');
    if (dbkeys.empty()) {
        LOG(INFO) << "keys empty";
        return;
    }
    if (values.empty()) {
        LOG(INFO) << "values empty";
        return;  
    }
    if (dbkeys.size() != values.size()) {
        LOG(INFO) << "key size not equal value size";
        return;   
    }

    proto::client::MSetMessageRequest request;
    for (int i = 0; i < dbkeys.size(); i++) {
        auto dbk = stringSplit(dbkeys[i], ':');
        if (dbk.size() != 2) {
            LOG(INFO) << "bad key format: " << dbkeys[i];
            return;
        }
        auto& value = trim(values[i]);

        auto subReq = request.mutable_datas()->Add();
        subReq->set_db_name(dbk[0]);
        subReq->set_key(dbk[1]);
        subReq->set_ttl(FLAGS_set_ttl);
        subReq->set_type(::proto::element::ElementType::STRING);
        subReq->set_s(value);
    }

    if (request.datas_size() == 0) {
        LOG(INFO) << "empty set data";
        return;
    }

    LOG(INFO) << "request: " << request.DebugString();
    proto::client::MSetMessageResponse response;
    auto err = cluster->mset(request, response);
    if (err) LOG(ERROR) << err.what();
    else LOG(INFO) << "response: " << response.DebugString();
}