#include "command_set.h"
#include <client.pb.h>

DECLARE_string(set_db);
DECLARE_string(set_key);
DECLARE_string(set_value);
DECLARE_int64(set_ttl); // ms

void chakra::cli::CommandSet::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    std::string& value = trim(FLAGS_set_value);
    int count = 0, dot = 0;
    for (int i = 0; i < value.size(); i++) {
        if (::isdigit(value.at(i))) {
            count++;
        }
        if (value.at(i) == '.') {
            count++;
            dot++;
        }
    }
    
    proto::client::SetMessageRequest setMessageRequest;
    setMessageRequest.set_db_name(FLAGS_set_db);
    setMessageRequest.set_key(FLAGS_set_key);
    setMessageRequest.set_ttl(FLAGS_set_ttl);

    proto::client::SetMessageResponse setMessageResponse;
    error::Error err;
    if (count == value.size() && dot <= 1) {
        int pos = value.find('.');
        float v = 0;
        if (pos > 0) { // float
            v = std::atof(value.c_str());
        } else { // int
            v = std::atoi(value.c_str());
        }
        setMessageRequest.set_f(v);
        setMessageRequest.set_type(::proto::element::ElementType::FLOAT);
        err = cluster->set(setMessageRequest, setMessageResponse);
    } else {
        setMessageRequest.set_s(value);
        setMessageRequest.set_type(::proto::element::ElementType::STRING);
        err = cluster->set(setMessageRequest, setMessageResponse);
    }
    if (err) LOG(ERROR) << err.what();
}