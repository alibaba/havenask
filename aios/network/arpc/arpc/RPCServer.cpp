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
#include "aios/network/arpc/arpc/RPCServer.h"

#include <algorithm>
#include <google/protobuf/service.h>
#include <list>
#include <map>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/PacketArg.h"
#include "aios/network/arpc/arpc/RPCServerAdapter.h"
#include "aios/network/arpc/arpc/RPCServerList.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "autil/LockFreeThreadPool.h"

using namespace std;
using namespace anet;
ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(RPCServer);

RPCServer::RPCServer(size_t threadNum, size_t queueSize)
    : _serverAdapter(nullptr), _messageCodec(nullptr), _sharedThreadPoolName("") {
    _isStopped = false;
    _version = ARPC_VERSION_CURRENT;
    _defaultThreadNum = threadNum;
    _defaultQueueSize = queueSize;
    addRPCServerToList(this);
}

RPCServer::~RPCServer() {
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

bool RPCServer::RegisterService(RPCService *rpcService, ThreadPoolDescriptor threadPoolDescriptor) {
    std::string threadPoolName = "";
    bool overrided = false;
    if (threadPoolDescriptor._allowSharedPoolOverride) {
        autil::ScopedReadLock lock(_sharedThreadPoolLock);
        if (_poolOverride) {
            threadPoolName = _sharedThreadPoolName;
            overrided = true;
            ARPC_LOG(INFO,
                     "Override the thread pool %s by shared thread pool %s",
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
        auto threadPool = GetThreadPool(threadPoolDescriptor.threadPoolName);
        autil::ScopedWriteLock lock(_serviceThreadPoolLock);
        _serviceThreadPoolMap[rpcService] = threadPool;
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
        auto threadPool = GetThreadPool(pool->getName());
        autil::ScopedWriteLock lock(_serviceThreadPoolLock);
        _serviceThreadPoolMap[rpcService] = threadPool;
        return true;
    }
    return false;
}

bool RPCServer::RegisterService(const std::shared_ptr<RPCService> &rpcService,
                                const std::string &methodName,
                                ThreadPoolDescriptor threadPoolDescriptor) {
    bool ret = addAndStartThreadPool(threadPoolDescriptor);

    if (!ret) {
        ARPC_LOG(ERROR, "start thread pool failed");
        return false;
    }

    if (doRegisterService(rpcService, methodName)) {
        auto threadPool = GetThreadPool(threadPoolDescriptor.threadPoolName);
        autil::ScopedWriteLock lock(_serviceThreadPoolLock);
        _serviceThreadPoolMap[rpcService.get()] = threadPool;
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

autil::ThreadPoolBasePtr RPCServer::GetThreadPool(const string &threadPoolName) const {
    autil::ScopedReadLock lock(_threadPoolLock);
    ThreadPoolMap::const_iterator it = _threadPoolMap.find(threadPoolName);

    if (it != _threadPoolMap.end()) {
        return it->second;
    }

    return autil::ThreadPoolBasePtr();
}

std::vector<std::string> RPCServer::GetThreadPoolNames() const {
    autil::ScopedReadLock lock(_threadPoolLock);
    std::vector<std::string> threadPoolNames;

    for (auto &item : _threadPoolMap) {
        threadPoolNames.emplace_back(item.first);
    }

    return threadPoolNames;
}

autil::ThreadPoolBasePtr RPCServer::GetServiceThreadPool(RPCService *rpcService) const {
    autil::ScopedReadLock lock(_serviceThreadPoolLock);
    ServiceThreadPoolMap::const_iterator it = _serviceThreadPoolMap.find(rpcService);

    if (it != _serviceThreadPoolMap.end()) {
        return it->second;
    }

    return autil::ThreadPoolBasePtr();
}

bool RPCServer::addAndStartThreadPool(const ThreadPoolDescriptor &desc) {
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

void RPCServer::stopThreadPools() {
    autil::ScopedWriteLock lock(_threadPoolLock);
    ThreadPoolMap::iterator it = _threadPoolMap.begin();

    for (; it != _threadPoolMap.end(); ++it) {
        it->second->stop();
    }

    _threadPoolMap.clear();
}

void RPCServer::removeServiceThreadPool(RPCService *rpcService) {
    {
        autil::ScopedWriteLock lock(_serviceThreadPoolLock);
        _serviceThreadPoolMap.erase(rpcService);
    }
    reconstructThreadPoolMap();
}

void RPCServer::reconstructThreadPoolMap() {
    ServiceThreadPoolMap serviceMap;
    {
        autil::ScopedReadLock lock(_serviceThreadPoolLock);
        serviceMap = _serviceThreadPoolMap;
    }
    ThreadPoolMap newThreadPoolMap;
    for (const auto &pair : serviceMap) {
        const auto &threadPool = pair.second;
        const auto &name = threadPool->getName();
        newThreadPoolMap.emplace(name, threadPool);
    }
    ThreadPoolMap swapMap;
    {
        autil::ScopedWriteLock lock(_threadPoolLock);
        swapMap.swap(_threadPoolMap);
        _threadPoolMap = newThreadPoolMap;
    }
    for (const auto &pair : swapMap) {
        const auto &threadPool = pair.second;
        if (1 == threadPool.use_count()) {
            threadPool->stop();
        }
    }
}

void RPCServer::StopServer() { _isStopped = true; }

void RPCServer::RecoverServer() { _isStopped = false; }

void RPCServer::dump(ostringstream &out) {
    out << "================================Dump of RPCServer===============================" << endl
        << "RPCServer Address: " << this << endl;

    // dump thread pool
    string leadingspaces = "    ";
    {
        autil::ScopedReadLock lock(_threadPoolLock);
        out << "ThreadPool Count: " << _threadPoolMap.size() << endl;
        int threadPoolIdx = 0;

        for (ThreadPoolMap::iterator it = _threadPoolMap.begin(); it != _threadPoolMap.end(); ++it) {
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

        for (ServiceThreadPoolMap::iterator it = _serviceThreadPoolMap.begin(); it != _serviceThreadPoolMap.end();
             ++it) {
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

RPCServer::ServiceMethodPair RPCServer::GetRpcCall(const CallId &callId) const {
    autil::ScopedReadLock lock(_mutex);
    auto it = _rpcCallMap.find(callId.intId);
    if (it == _rpcCallMap.end()) {
        ARPC_LOG(ERROR, "can not find the rpccall, callId: [%d]", callId.intId);
        return {{nullptr, nullptr}, nullptr};
    }
    return it->second;
}

bool RPCServer::doRegisterService(RPCService *rpcService) {
    const RPCServiceDescriptor *pSerDes = rpcService->GetDescriptor();
    int methodCnt = pSerDes->method_count();
    RPCCallMap newMap;
    for (int i = 0; i < methodCnt; i++) {
        RPCMethodDescriptor *pMethodDes = (RPCMethodDescriptor *)(pSerDes->method(i));
        uint32_t rpcCode = PacketCodeBuilder()(pMethodDes);
        auto it = newMap.find(rpcCode);

        if (it != newMap.end()) {
            ARPC_LOG(ERROR,
                     "duplicate rpcCode[%d], serviceName[%s], MethodName[%s]",
                     rpcCode,
                     pSerDes->name().c_str(),
                     pMethodDes->name().c_str());
            return false;
        }
        newMap[rpcCode] = {{nullptr, rpcService}, pMethodDes};
    }
    autil::ScopedWriteLock lock(_mutex);
    auto currentMap = _rpcCallMap;
    for (const auto &pair : currentMap) {
        auto rpcCode = pair.first;
        auto oldService = pair.second.first.second;
        auto oldMethodDesc = pair.second.second;
        auto it = newMap.find(rpcCode);
        if (newMap.end() != it) {
            auto newService = it->second.first.second;
            auto newMethodDesc = it->second.second;
            ARPC_LOG(INFO,
                     "rpcCode [%d] replaced, service [%s] [%p] method [%s] replaced by new service [%s] [%p] "
                     "method [%s]",
                     pair.first,
                     oldService->GetDescriptor()->full_name().c_str(),
                     oldService,
                     oldMethodDesc->full_name().c_str(),
                     newService->GetDescriptor()->full_name().c_str(),
                     newService,
                     newMethodDesc->full_name().c_str());
            continue;
        }
        newMap.emplace(pair);
    }
    _rpcCallMap = newMap;
    return true;
}

bool RPCServer::doRegisterService(const std::shared_ptr<RPCService> &rpcService, const std::string &methodName) {
    auto serviceDesc = rpcService->GetDescriptor();
    auto methodDesc = serviceDesc->FindMethodByName(methodName);
    if (!methodDesc) {
        ARPC_LOG(ERROR, "method [%s] not exist in service def", methodName.c_str());
        return false;
    }
    uint32_t rpcCode = PacketCodeBuilder()(methodDesc);
    auto currentMap = GetRPCCallMap();
    auto it = currentMap.find(rpcCode);
    if (currentMap.end() != it) {
        auto oldService = it->second.first.second;
        auto oldMethodDesc = it->second.second;
        ARPC_LOG(INFO,
                 "rpcCode [%d] replaced, service [%s] [%p] method [%s] replaced by new service [%s] [%p] method [%s]",
                 rpcCode,
                 oldService->GetDescriptor()->full_name().c_str(),
                 oldService,
                 oldMethodDesc->full_name().c_str(),
                 rpcService->GetDescriptor()->full_name().c_str(),
                 rpcService.get(),
                 methodDesc->full_name().c_str());
    }
    currentMap[rpcCode] = {{rpcService, rpcService.get()}, (RPCMethodDescriptor *)methodDesc};
    SetRPCCallMap(currentMap);
    return true;
}

void RPCServer::unRegisterService(const std::shared_ptr<RPCService> &rpcService) {
    RPCCallMap newMap;
    auto currentMap = GetRPCCallMap();
    for (const auto &pair : currentMap) {
        auto service = pair.second.first.first;
        if (service == rpcService) {
            removeServiceThreadPool(service.get());
            continue;
        }
        newMap.emplace(pair);
    }
    SetRPCCallMap(newMap);
}

RPCServer::RPCCallMap RPCServer::GetRPCCallMap() const {
    autil::ScopedReadLock lock(_mutex);
    return _rpcCallMap;
}

void RPCServer::SetRPCCallMap(const RPCCallMap &newMap) {
    autil::ScopedWriteLock lock(_mutex);
    _rpcCallMap = newMap;
}

ARPC_END_NAMESPACE(arpc);
