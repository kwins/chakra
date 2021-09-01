//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_ELEMENT_H
#define CHAKRA_ELEMENT_H

#include "db_object.h"
#include <rocksdb/db.h>

namespace chakra::database{

class Element {
public:
    void serialize(const std::function<void(char* ptr, size_t len)>& f);
    void deSeralize(const char* ptr, size_t len);

    bool isUpdated() const;
    void setUpdated(bool is);
    long getCreate() const;
    void setCreate(long createTimeMs);
    long getLastVisit() const;
    void setLastVisit(long lastVisitMs);
    long getExpire() const;
    void setExpire(long expireTimeMs);
    const std::shared_ptr<Object> &getObject() const;
    void setObject(const std::shared_ptr<Object> &objptr);

private:
    bool updated;
    long create;
    long last_visit;
    long expire;
    std::shared_ptr<Object> ptr;
};

}



#endif //CHAKRA_ELEMENT_H
