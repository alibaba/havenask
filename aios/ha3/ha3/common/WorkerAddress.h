#ifndef ISEARCH_WORKERADDRESS_H
#define ISEARCH_WORKERADDRESS_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(common);

class WorkerAddress
{
public:
    WorkerAddress(const std::string &address);
    WorkerAddress(const std::string &ip, uint16_t port);
    ~WorkerAddress();
public:
    const std::string& getAddress() const {return _address;}
    const std::string& getIp() const {return _ip;}
    uint16_t getPort() const {return _port;}
private:
    std::string _address;
    std::string _ip;
    uint16_t _port;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(WorkerAddress);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_WORKERADDRESS_H
