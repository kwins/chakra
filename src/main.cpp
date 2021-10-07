#include "service/chakra.h"
#include <gflags/gflags.h>
#include <glog/logging.h>

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

//    google::InitGoogleLogging(argv[0]);
    chakra::serv::Chakra::get()->startUp();

    gflags::ShutDownCommandLineFlags();
//    google::ShutdownGoogleLogging();
    LOG(INFO) << "end";
    return 0;
}
