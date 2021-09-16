#include "service/chakra.h"
#include <gflags/gflags.h>
#include <glog/logging.h>

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    chakra::serv::Chakra::get()->startUp();
    LOG(INFO) << "end";
    return 0;
}
