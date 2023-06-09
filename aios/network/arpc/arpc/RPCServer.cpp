/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <google/protobuf/service.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <list>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "autil/LockFreeThreadPool.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include "aios/network/arpc/arpc/RPCServerAdapter.h"
#include "aios/network/arpc/arpc/RPCServerList.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/arpc/arpc/PacketArg.h"

using namespace std;
using namespace anet;
ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(RPCServer);

RPCServer::RPCServer(size_t threadNum, size_t queueSize)
    : _serverAdapter(nullptr)
    , _messageCodec(nullptr)
    , _sharedThreadPoolName("")
{
    _isStopped = false;
    _version = ARPC_VERSION_CURRENT;
    _defaultThreadNum = threadNum;
    _defaultQueueSize = queueSize;
    addRPCServerToList(this);
}

RPCServer::~RPCServer()
{
    stopThreadPools();
    if (_serverAdapter) {
        delete _serverAdapter;
        _serverAdapter = nullptr;
    }
    if (_messageCodec) {
        delete _messageCodec;
        _messageCodec = nullptr;
    }
    delRPCServerFromList(this);
    _serviceThreadPoolMap.clear();
}

bool RPCServer::RegisterService(RPCService *rpcService,
                                ThreadPoolDescriptor threadPoolDescriptor)
{
    std::string threadPoolName = "";
    bool overrided = false;
    if (threadPoolDescriptor._allowSharedPoolOverride) {
        autil::ScopedReadLock lock(_sharedThreadPoolLock);
        if (_poolOverride) {
            threadPoolName = _sharedThreadPoolName;
            overrided = true;
            ARPC_LOG(INFO, "Override the thread pool %s by shared thread pool %s", 
                        threadPoolDescriptor.threadPoolName.c_str(), 
                        threadPoolName.c_str());
        }
    }
    
    if (!overrided) {
        threadPoolName = threadPoolDescriptor.threadPoolName;
        if (!addAndStartThreadPool(threadPoolDescriptor)) {
            ARPC_LOG(ERROR, "start thread pool failed");
            return false;
        }
    }

    if (doRegisterService(rpcService)) {
        autil::ScopedWriteLock lock(_serviceThreadPoolLock);
        _serviceThreadPoolMap[rpcService] =
            GetThreadPool(threadPoolName);
        return true;
    }

    return false;
}

bool RPCServer::RegisterService(RPCService *rpcService, const autil::ThreadPoolBasePtr &pool) {
    bool ret = RegisterThreadPool(pool);
    if (!ret) {
        ARPC_LOG(ERROR, "register thread pool failed");
        return false;
    }
    if (doRegisterService(rpcService)) {
        autil::ScopedWriteLock lock(_serviceThreadPoolLock);
        _serviceThreadPoolMap[rpcService] =
            GetThreadPool(pool->getName());
        return true;
    }
    return false;
}

bool RPCServer::RegisterThreadPool(const autil::ThreadPoolBasePtr &pool) {
    if (!pool) {
        return false;
    }
    {
        autil::ScopedWriteLock lock(_threadPoolLock);
        _threadPoolMap[pool->getName()] = pool;
    }
    {
        autil::ScopedWriteLock lock(_sharedThreadPoolLock);
        _sharedThreadPoolName = pool->getName();
        _poolOverride = true;
    }
    return true;
}

autil::ThreadPoolBasePtr RPCServer::GetThreadPool(const string &threadPoolName) const
{
    autil::ScopedReadLock lock(_threadPoolLock);
    ThreadPoolMap::const_iterator it = _threadPoolMap.find(threadPoolName);

    if (it != _threadPoolMap.end()) {
        return it->second;
    }

    return autil::ThreadPoolBasePtr();
}

std::vector<std::string> RPCServer::GetThreadPoolNames() const
{
    autil::ScopedReadLock lock(_threadPoolLock);
    std::vector<std::string> threadPoolNames;

    for (auto &item: _threadPoolMap) {
        threadPoolNames.emplace_back(item.first);
    }

    return threadPoolNames;
}

autil::ThreadPoolBasePtr RPCServer::GetServiceThreadPool(RPCService *rpcService) const
{
    autil::ScopedReadLock lock(_serviceThreadPoolLock);
    ServiceThreadPoolMap::const_iterator it =
        _serviceThreadPoolMap.find(rpcService);

    if (it != _serviceThreadPoolMap.end()) {
        return it->second;
    }

    return autil::ThreadPoolBasePtr();
}

bool RPCServer::addAndStartThreadPool(const ThreadPoolDescriptor& desc)
{
    autil::ThreadPoolBasePtr threadPoolPtr = GetThreadPool(desc.threadPoolName);

    if (threadPoolPtr != NULL) {
        return true;
    }

    size_t threadNum = desc.threadNum;
    size_t queueSize = desc.queueSize;
    if (desc.threadPoolName == DEFAULT_TREAHDPOOL_NAME) {
        threadNum = _defaultThreadNum;
        queueSize = _defaultQueueSize;
    }
    threadPoolPtr.reset(new autil::LockFreeThreadPool(threadNum, queueSize, desc.factory, desc.threadPoolName));
    if (!threadPoolPtr->start()) {
        ARPC_LOG(ERROR, "start thread pool failed");
        return false;
    }

    autil::ScopedWriteLock lock(_threadPoolLock);
    _threadPoolMap[desc.threadPoolName] = threadPoolPtr;
    return true;
}

void RPCServer::stopThreadPools()
{
    autil::ScopedWriteLock lock(_threadPoolLock);
    ThreadPoolMap::iterator it = _threadPoolMap.begin();

    for (; it != _threadPoolMap.end(); ++it) {
        it->second->stop();
    }

    _threadPoolMap.clear();
}

void RPCServer::StopServer()
{
    _isStopped = true;
}

void RPCServer::RecoverServer()
{
    _isStopped = false;
}

void RPCServer::dump(ostringstream &out)
{
    out << "================================Dump of RPCServer===============================" << endl
        << "RPCServer Address: " << this << endl;

    // dump thread pool
    string leadingspaces = "    ";
    {
        autil::ScopedReadLock lock(_threadPoolLock);
        out << "ThreadPool Count: " << _threadPoolMap.size() << endl;
        int threadPoolIdx = 0;

        for (ThreadPoolMap::iterator it = _threadPoolMap.begin();
                it != _threadPoolMap.end(); ++it) {
            out << leadingspaces << "ThreadPool " << threadPoolIdx++ << ": " << it->first << endl;
            it->second->dump(leadingspaces, out);
        }
    }

    // dump service
    string methodspaces = leadingspaces + leadingspaces;
    {
        autil::ScopedReadLock lock(_serviceThreadPoolLock);
        out << "Service Count: " << _serviceThreadPoolMap.size() << endl;
        int serviceIdx = 0;

        for (ServiceThreadPoolMap::iterator it = _serviceThreadPoolMap.begin();
                it != _serviceThreadPoolMap.end(); ++it) {
            const RPCServiceDescriptor *pSerDes = it->first->GetDescriptor();
            int methodCnt = pSerDes->method_count();
            out << leadingspaces << "Service " << serviceIdx << ": " << pSerDes->name() << endl
                << leadingspaces << "ThreadPool Addr: 0x" << it->second << endl
                << leadingspaces << "Method Count: " << methodCnt << endl;

            for (int i = 0; i < methodCnt; i++) {
                RPCMethodDescriptor *pMethodDes = (RPCMethodDescriptor *)(pSerDes->method(i));
                out << methodspaces << "Method " << i << ": " << pMethodDes->name() << endl;
            }
        }
    }
}

RPCServer::ServiceMethodPair RPCServer::GetRpcCall( const CallId &callId) const {
    RPCCallMap::const_iterator it = _rpcCallMap.find(callId.intId);

    if (it == _rpcCallMap.end()) {
        ARPC_LOG(ERROR, "can not find the rpccall, callId: [%d]", callId.intId);
        return ServiceMethodPair(NULL, NULL);
    }

    return it->second;
}

bool RPCServer::doRegisterService(RPCService *rpcService)
{
    const RPCServiceDescriptor *pSerDes = rpcService->GetDescriptor();
    int methodCnt = pSerDes->method_count();

    for (int i = 0; i < methodCnt; i++) {
        RPCMethodDescriptor *pMethodDes
            = (RPCMethodDescriptor *)(pSerDes->method(i));
        uint32_t rpcCode = PacketCodeBuilder()(pMethodDes);
        RPCCallMap::const_iterator it = _rpcCallMap.find(rpcCode);

        if (it != _rpcCallMap.end()) {
            ARPC_LOG(ERROR, "duplicate rpcCode[%d], serviceName[%s], MethodName[%s]",
                     rpcCode,
                     pSerDes->name().c_str(),
                     pMethodDes->name().c_str());
            return false;
        }

        _rpcCallMap[rpcCode] = make_pair(rpcService, pMethodDes);
    }
    return true;
}

ARPC_END_NAMESPACE(arpc);
