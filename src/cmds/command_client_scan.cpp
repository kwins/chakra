#include "command_client_scan.h"
#include "client.pb.h"
#include "database/db_family.h"
#include "net/packet.h"
#include <chrono>
#include <cluster/cluster.h>
#include <cstddef>
#include <exception>
#include <rocksdb/options.h>
#include <service/chakra.h>
#include <types.pb.h>

void chakra::cmds::CommandClientScan::execute(char *req, size_t len, void *data, std::function<error::Error (char *, size_t)> cbf) {
    proto::client::ScanMessageRequest scanMessageRequest;
    proto::client::ScanMessageResponse scanMessageResponse;
    
    auto err = chakra::net::Packet::deSerialize(req, len, scanMessageRequest, proto::types::C_SCAN);
    if (err) {
        chakra::net::Packet::fillError(scanMessageResponse.mutable_error(), 1, err.what());
    } else if (scanMessageRequest.batch_size() <= 0 || scanMessageRequest.dbname().empty()) {
        chakra::net::Packet::fillError(scanMessageResponse.mutable_error(), 1, "batch size zero or dbname empty");
    } else {

        try {
            auto dbptr = chakra::database::FamilyDB::get();
            rocksdb::ReadOptions readOptions;
            auto iterator = dbptr->iterator(scanMessageRequest.dbname(), readOptions);
            if (scanMessageRequest.start().empty()) {
                iterator->SeekToFirst();
            } else {
                iterator->Seek(scanMessageRequest.start());
                iterator->Next(); // 从下一个开始
            }
            
            while (iterator->Valid()) {
                if (scanMessageResponse.elements_size() >= scanMessageRequest.batch_size()) {
                    break;
                }
                auto element = scanMessageResponse.mutable_elements()->Add();
                element->ParseFromString(iterator->value().ToString());
                iterator->Next();
            }

            auto clsptr = chakra::cluster::Cluster::get();
            scanMessageResponse.set_peername(clsptr->getMyself()->getName());
            if (iterator->Valid()) {
                scanMessageResponse.set_end(false);
            } else {
                scanMessageResponse.set_end(true);
            }
            delete iterator;
            iterator = nullptr;
        } catch (const std::exception& err) {
            LOG(ERROR) << "[chakra] scan new iterator error " << err.what();
            chakra::net::Packet::fillError(scanMessageResponse.mutable_error(), 1, err.what());
        }
    }
    chakra::net::Packet::serialize(scanMessageResponse, proto::types::C_SCAN, cbf);
}