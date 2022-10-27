#ifndef ISEARCH_FAKECHANNELMANAGER_H
#define ISEARCH_FAKECHANNELMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <arpc/ANetRPCChannelManager.h>
#include <ha3/service/test/FakeRpcChannel.h>
BEGIN_HA3_NAMESPACE(service);

class FakeChannelManager: public arpc::ANetRPCChannelManager
{
public:
    FakeChannelManager();
    ~FakeChannelManager();
private:
    FakeChannelManager(const FakeChannelManager &);
    FakeChannelManager& operator=(const FakeChannelManager &);
public:
    virtual RPCChannel* OpenChannel(const std::string &address, 
                                    bool block = false,
                                    size_t queueSize = 50ul,
                                    int timeout = 5000,
                                    bool autoReconn = true,
                                    anet::CONNPRIORITY prio = anet::ANET_PRIORITY_NORMAL)
    {
        return new FakeRpcChannel(&_threadPool);
    }
private:
    autil::ThreadPool _threadPool;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeChannelManager);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_FAKECHANNELMANAGER_H
