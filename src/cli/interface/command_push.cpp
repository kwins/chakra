#include "command_push.h"
#include <cassert>
#include <client.pb.h>
#include <element.pb.h>
#include <exception>
#include <gflags/gflags_declare.h>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

DECLARE_string(push_key);
DECLARE_string(push_values);
DECLARE_int64(push_ttl); // ms

void chakra::cli::CommandPush::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    if (FLAGS_push_key.empty()) {
        LOG(ERROR) << "push key empty";
        return;
    }
    auto value = trim(FLAGS_push_values);
    if (value.empty()) {
        LOG(ERROR) << "push value empty";
        return;
    }

    std::vector<std::string> dbk = stringSplit(FLAGS_push_key, ':');
    assert(dbk.size() == 2);
    proto::client::PushMessageRequest request;
    request.set_db_name(dbk[0]);
    request.set_key(dbk[1]);
    request.set_ttl(FLAGS_push_ttl);

    std::vector<std::string> values = stringSplit(value, ',');
    int numf = 0, nums = 0;
    for(auto& v : values) {
        try {
            float rv = std::stof(v);
            numf++;
        } catch (std::invalid_argument& e1) { //  不能转换
            nums++;
        } catch (std::exception & e2) { // 转换异常
            LOG(ERROR) << "push parse value error:" << e2.what();
            return;
        }
    }

    if (numf > 0 && numf == values.size()) {
        request.set_type(::proto::element::ElementType::FLOAT_ARRAY);
        for(auto& v : values) {
            request.mutable_ff()->add_value(std::stof(v));
        }
    } else if (nums > 0 && nums == values.size()) {
        request.set_type(::proto::element::ElementType::STRING_ARRAY);
        for(auto& v : values) {
            request.mutable_ss()->add_value(v);
        }
    } else {
        LOG(ERROR) << "push value must be the same type";
        return;
    }

    proto::client::PushMessageResponse response;
    auto err = cluster->push(request, response);
    if (err) LOG(ERROR) << err.what();
    else LOG(INFO) << "response: " << response.DebugString();
}