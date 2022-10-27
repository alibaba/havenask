#include "build_service/util/PooledChannelManager.h"
#include <arpc/ANetRPCChannel.h>
using namespace std;
using namespace arpc;
using namespace autil;

namespace build_service {
namespace util {
BS_LOG_SETUP(util, PooledChannelManager);

PooledChannelManager::PooledChannelManager() {
    _rpcChannelManager = new ANetRPCChannelManager();
    _rpcChannelManager->StartPrivateTransport();
}

PooledChannelManager::~PooledChannelManager() {
    _loopThreadPtr.reset();
    _rpcChannelPool.clear();

    _rpcChannelManager->StopPrivateTransport();
    DELETE_AND_SET_NULL(_rpcChannelManager);
}

RPCChannelPtr PooledChannelManager::getRpcChannel(const std::string &addr) {
    autil::ScopedLock lock(_poolMutex);
    if (_loopThreadPtr.get() == NULL) {
        _loopThreadPtr = LoopThread::createLoopThread(
                tr1::bind(&PooledChannelManager::cleanBrokenChannelLoop, this), 10 * 1000 * 1000);
    }
    map<string, RPCChannelPtr>::iterator it = _rpcChannelPool.find(addr);
    if (it != _rpcChannelPool.end()) {
        ANetRPCChannel *ch = static_cast<ANetRPCChannel *>(it->second.get());
        if (ch->ChannelBroken()) {
            _rpcChannelPool.erase(it);
            return RPCChannelPtr();
        } else {
            return it->second;
        }
    }
    
    RPCChannelPtr rpcChannel = createRpcChannel(addr);
    if (rpcChannel) {
        _rpcChannelPool[addr] = rpcChannel;
    }
    
    return rpcChannel;
}

void PooledChannelManager::cleanBrokenChannelLoop() {
    autil::ScopedLock lock(_poolMutex);
    for (auto it = _rpcChannelPool.begin(); it != _rpcChannelPool.end();) {
        ANetRPCChannel *ch = static_cast<ANetRPCChannel *>(it->second.get());
        if (ch->ChannelBroken()) {
            _rpcChannelPool.erase(it++);
        } else {
            it++;
        }
    }
}

RPCChannelPtr PooledChannelManager::createRpcChannel(const string &addr) {
    string spec = convertToSpec(addr);
    bool block = false;
    size_t queueSize = 50ul;
    int timeout = 5000;
    bool autoReconnect = false;
    RPCChannelPtr rpcChannel(_rpcChannelManager->OpenChannel(spec,
                    block, queueSize, timeout, autoReconnect));
    if (rpcChannel == NULL) {
        BS_LOG(ERROR, "open channel on [%s] failed", spec.c_str());
    }
    return rpcChannel; 
}

string PooledChannelManager::convertToSpec(const string &addr) {
    if (StringUtil::startsWith(addr, "tcp:") 
        || StringUtil::startsWith(addr, "udp:")
        || StringUtil::startsWith(addr, "http:"))
    {
        return addr;
    } else {
        return "tcp:" + addr;
    }
}

}
}
