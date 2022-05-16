#include "interface/cli.h"

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    chakra::cli::Cli cli;
    cli.execute();
    gflags::ShutDownCommandLineFlags();
}