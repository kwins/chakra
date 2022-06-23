//
// Created by kwins on 2021/5/21.
//

#include "basic.h"

#include <chrono>
#include <cstring>
#include <gflags/gflags_declare.h>
#include <random>
#include <algorithm>
#include <gflags/gflags.h>
#include <unistd.h>/* gethostname */
#include <netdb.h> /* struct hostent */
#include <arpa/inet.h> /* inet_ntop */
#include <glog/logging.h>
#include "error/err.h"

DECLARE_int32(server_port);

std::string chakra::utils::Basic::genRandomID(size_t len) {
    if (len < 16) len = 16;
    static const char alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    std::random_device rd;
    std::default_random_engine rng(rd());
    std::uniform_int_distribution<> dist(0,25);

    std::string str;
    std::generate_n(std::back_inserter(str), len - 14, [&]{
       return alphabet[dist(rng)];
    });

    return std::to_string(getNowMillSec()) + "-" + str;
}

long chakra::utils::Basic::getNowSec() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::seconds>(now).time_since_epoch().count();

}

long chakra::utils::Basic::getNowMillSec() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();
}

uint64_t chakra::utils::Basic::getNowMicroSec() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::microseconds>(now).time_since_epoch().count();
}

std::string chakra::utils::Basic::getLocalIP() {
	char name[256];
	gethostname(name, sizeof(name));	
	struct hostent* host = gethostbyname(name);
	char ip[32];
	const char* ret = inet_ntop(host->h_addrtype, host->h_addr_list[0], ip, sizeof(ip));
	if (ret == nullptr) {
		throw error::Error(strerror(errno));
	}
	return ip;
}

int32_t chakra::utils::Basic::sport() { return FLAGS_server_port; }
int32_t chakra::utils::Basic::cport() { return FLAGS_server_port + 1; }
int32_t chakra::utils::Basic::rport() { return FLAGS_server_port + 2;}
