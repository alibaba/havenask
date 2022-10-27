#ifndef ISEARCH_ZKSERVER_H
#define ISEARCH_ZKSERVER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(util);

class ZkServer
{
private:
    ZkServer();
    ~ZkServer();
private:
    ZkServer(const ZkServer &);
    ZkServer& operator = (const ZkServer &);
public:
    static ZkServer * getZkServer();
    unsigned short start();
    void stop();
    unsigned short port() const {
        return _port;
    }
    unsigned short simpleStart();
private:
    unsigned short getPort(unsigned short from = 1025, unsigned short to = 60001);
    unsigned short choosePort();
private:
    unsigned short _port;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ZkServer);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_ZKSERVER_H
