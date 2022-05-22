#include "command_set.h"

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
    
    error::Error err;
    if (count == value.size() && dot <= 1) {
        int pos = value.find('.');
        if (pos > 0) { // float
            float f = std::atof(value.c_str());
            err = cluster->set(FLAGS_set_db, FLAGS_set_key, f, FLAGS_set_ttl);
        } else { // int
            int64_t i = std::atoi(value.c_str());
            err = cluster->set(FLAGS_set_db, FLAGS_set_key, i, FLAGS_set_ttl);
        }
    } else {
        err = cluster->set(FLAGS_set_db,FLAGS_set_key, value, FLAGS_set_ttl);
    }
    if (err) LOG(ERROR) << err.what();
}