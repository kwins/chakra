//
// Created by kwins on 2021/6/9.
//

#include "element.h"
#include "type_string.h"
#include <glog/logging.h>

void chakra::database::Element::serialize(const std::function<void(char *, size_t)> &f) {
    ptr->serialize([this, &f](char* body, size_t bodyLen){
        size_t head =  sizeof(ptr->type()) + sizeof(create)
                + sizeof(last_visit) + sizeof(expire);

        size_t size = head + bodyLen;
        char ch[size];
        int pos = 0;
        uint8_t type = ptr->type();
        memcpy(&ch[pos], (char*)&type, sizeof(type));
        pos += sizeof(type);

        memcpy(&ch[pos], (char*)&create, sizeof(create));
        pos += sizeof(create);

        memcpy(&ch[pos], (char*)&last_visit, sizeof(last_visit));
        pos += sizeof(last_visit);

        memcpy(&ch[pos], (char*)&expire, sizeof(expire));
        pos += sizeof(expire);

        memcpy(&ch[pos], body, bodyLen);

        f(&ch[0], size);
    });
}

void chakra::database::Element::deSeralize(const char *data, size_t dataLen) {
    int pos = 0;
    uint8_t type = *((uint8_t*)&data[pos]);
    pos += sizeof(type);

    create = *((long*)&data[pos]);
    pos += sizeof(create);

    last_visit = *((long*)&data[pos]);
    pos += sizeof(last_visit);

    expire = *((long*)&data[pos]);
    pos += sizeof(expire);

    if (type == chakra::database::Object::Type::STRING){
        ptr = std::make_shared<chakra::database::String>();
    }else {
        LOG(ERROR) << "Element deSeralize bad type " << type;
        return;
    }
    ptr->deSeralize(&data[pos], dataLen - pos);
}

bool chakra::database::Element::isUpdated() const { return updated; }

void chakra::database::Element::setUpdated(bool is) { Element::updated = is; }

long chakra::database::Element::getCreate() const { return create; }

void chakra::database::Element::setCreate(long createTimeMs) { Element::create = createTimeMs; }

long chakra::database::Element::getLastVisit() const { return last_visit; }

void chakra::database::Element::setLastVisit(long lastVisitMs) { last_visit = lastVisitMs; }

long chakra::database::Element::getExpire() const { return expire; }

void chakra::database::Element::setExpire(long expireTimeMs) { Element::expire = expireTimeMs; }

const std::shared_ptr<chakra::database::Object> &chakra::database::Element::getObject() const { return ptr;}

void chakra::database::Element::setObject(const std::shared_ptr<Object> &objptr) { Element::ptr = objptr; }
