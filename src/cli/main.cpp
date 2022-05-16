#include "interface/cli.h"

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    chakra::cli::Cli cli;
    cli.execute();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    gflags::ShutDownCommandLineFlags();
}