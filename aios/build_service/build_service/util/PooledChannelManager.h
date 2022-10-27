#ifndef ISEARCH_BS_POOLEDCHANNELMANAGER_H
#define ISEARCH_BS_POOLEDCHANNELMANAGER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <arpc/ANetRPCChannelManager.h>
#include <autil/Lock.h>
#include <autil/LoopThread.h>
namespace build_service {
namespace util {

BS_TYPEDEF_PTR(RPCChannel);

class PooledChannelManager
{
public:
    PooledChannelManager();
    virtual ~PooledChannelManager();
private:
    PooledChannelManager(const PooledChannelManager &);
    PooledChannelManager& operator=(const PooledChannelManager &);
public:
    virtual RPCChannelPtr getRpcChannel(const std::string &addr);
private:
    RPCChannelPtr createRpcChannel(const std::string &spec);
    void cleanBrokenChannelLoop();
private:
    static std::string convertToSpec(const std::string &addr);
private:
    autil::ThreadMutex _poolMutex;
    arpc::ANetRPCChannelManager *_rpcChannelManager;
    std::map<std::string, RPCChannelPtr> _rpcChannelPool;
    autil::LoopThreadPtr _loopThreadPtr;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PooledChannelManager);

}
}

#endif //ISEARCH_BS_POOLEDCHANNELMANAGER_H
