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
#include "aios/network/http_arpc/HTTPRPCServer.h"

#include "aios/autil/autil/Lock.h"
#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include "aios/network/http_arpc/HTTPANetApp.h"
#include "aios/network/http_arpc/HTTPRPCServerAdapter.h"
#include "aios/network/http_arpc/HTTPRPCServerWorkItem.h"
#include "aios/network/http_arpc/Log.h"
#include "autil/LockFreeThreadPool.h"

using namespace std;
using namespace anet;
using namespace arpc;
using namespace autil;

#define DEFAULT_HTTP_TREAHDPOOL_NAME "HttpArpc"

namespace http_arpc {

HTTP_ARPC_DECLARE_AND_SETUP_LOGGER(HTTPRPCServer);

class HTTPRPCServer;

class HTTPRPCServerImpl {
public:
    HTTPRPCServerImpl(ANetRPCServer *rpcServer,
                      HTTPRPCServer *httpRpcServer,
                      Transport *transport,
                      size_t threadNum,
                      size_t queueSize,
                      bool decodeUri,
                      bool haCompatible)
        : _anetApp(transport)
        , _rpcServer(rpcServer)
        , _defaultThreadNum(threadNum)
        , _defaultQueueSize(queueSize)
        , _decodeUri(decodeUri)
        , _haCompatible(haCompatible)
        , _serverAdapter(httpRpcServer) {}
    ~HTTPRPCServerImpl() {}

public:
    void addDefaultThreadPool() {
        autil::ScopedLock lock(_threadPoolMutex);
        ThreadPoolMap::const_iterator it = _threadPoolMap.find(DEFAULT_HTTP_TREAHDPOOL_NAME);
        if (it != _threadPoolMap.end()) {
            return;
        }

        autil::ThreadPoolBasePtr threadPoolPtr(new autil::ThreadPool(
            _defaultThreadNum, _defaultQueueSize, autil::WorkItemQueueFactoryPtr(), DEFAULT_HTTP_TREAHDPOOL_NAME));

        _threadPoolMap[DEFAULT_HTTP_TREAHDPOOL_NAME] = threadPoolPtr;
    }

    autil::ThreadPoolBasePtr GetThreadPool(const string &threadPoolName) const {
        autil::ScopedLock lock(_threadPoolMutex);
        ThreadPoolMap::const_iterator it = _threadPoolMap.find(threadPoolName);
        if (it != _threadPoolMap.end()) {
            return it->second;
        }

        return autil::ThreadPoolBasePtr();
    }

    bool addAndStartThreadPool(const string &threadPoolName, size_t threadNum, size_t queueSize) {
        autil::ScopedLock lock(_threadPoolMutex);

        ThreadPoolMap::const_iterator it = _threadPoolMap.find(threadPoolName);
        if (it != _threadPoolMap.end()) {
            return true;
        }

        if (threadPoolName == DEFAULT_HTTP_TREAHDPOOL_NAME) {
            threadNum = _defaultThreadNum;
            queueSize = _defaultQueueSize;
        }

        autil::ThreadPoolBasePtr threadPoolPtr(
            new autil::ThreadPool(threadNum, queueSize, autil::WorkItemQueueFactoryPtr(), threadPoolName));

        if (!threadPoolPtr->start()) {
            ARPC_LOG(ERROR, "start thread pool failed");
            return false;
        }

        _threadPoolMap[threadPoolName] = threadPoolPtr;
        return true;
    }

    size_t getQueueSize(const std::string &name) {
        autil::ThreadPoolBasePtr threadPool = GetThreadPool(name);
        if (threadPool) {
            return threadPool->getQueueSize();
        } else {
            return 0;
        }
    }

    size_t getItemCount(const std::string &name) {
        autil::ThreadPoolBasePtr threadPool = GetThreadPool(name);
        if (threadPool) {
            return threadPool->getItemCount();
        } else {
            return 0;
        }
    }

    autil::ThreadPoolBasePtr GetServiceThreadPool(RPCService *rpcService) const {
        autil::ScopedLock lock(_serviceThreadPoolMutex);
        ServiceThreadPoolMap::const_iterator it = _serviceThreadPoolMap.find(rpcService);
        if (it != _serviceThreadPoolMap.end()) {
            return it->second;
        }

        return autil::ThreadPoolBasePtr();
    }

    void SetServiceThreadPool(RPCService *rpcService, const autil::ThreadPoolBasePtr &threadPool) {
        autil::ScopedLock lock(_serviceThreadPoolMutex);
        _serviceThreadPoolMap[rpcService] = threadPool;
    }

    void stopThreadPools() {
        autil::ScopedLock lock(_threadPoolMutex);
        ThreadPoolMap::iterator it = _threadPoolMap.begin();
        for (; it != _threadPoolMap.end(); ++it) {
            it->second->stop();
        }
        _threadPoolMap.clear();
    }

public:
    typedef std::map<std::string, autil::ThreadPoolBasePtr> ThreadPoolMap;
    typedef std::map<RPCService *, autil::ThreadPoolBasePtr> ServiceThreadPoolMap;

public:
    HTTPANetApp _anetApp;
    ANetRPCServer *_rpcServer;
    RPCNameMap _rpcNameMap;
    size_t _defaultThreadNum;
    size_t _defaultQueueSize;
    bool _decodeUri;
    bool _haCompatible;

    ThreadPoolMap _threadPoolMap;
    mutable autil::ThreadMutex _threadPoolMutex;
    ServiceThreadPoolMap _serviceThreadPoolMap;
    mutable autil::ThreadMutex _serviceThreadPoolMutex;

    HTTPRPCServerAdapter _serverAdapter;
    autil::ThreadMutex _ioComponentMutex;
    list<IOComponent *> _ioComponentList;
};

HTTPRPCServer::HTTPRPCServer(ANetRPCServer *rpcServer, Transport *transport, size_t threadNum, size_t queueSize)
    : _impl(new HTTPRPCServerImpl(rpcServer, this, transport, threadNum, queueSize, false, false)) {
    _impl->addDefaultThreadPool();
    registerService();
}

HTTPRPCServer::HTTPRPCServer(ANetRPCServer *rpcServer,
                             Transport *transport,
                             size_t threadNum,
                             size_t queueSize,
                             bool decodeUri,
                             bool haCompatible)
    : _impl(new HTTPRPCServerImpl(rpcServer, this, transport, threadNum, queueSize, decodeUri, haCompatible)) {
    _impl->addDefaultThreadPool();
    registerService();
}

HTTPRPCServer::~HTTPRPCServer() {
    Close();
    delete _impl;
}

void HTTPRPCServer::registerService() {
    if (!_impl->_rpcServer) {
        return;
    }
    {
        ScopedWriteLock lock(_methodMapMutex);
        const ANetRPCServer::RPCCallMap &rpcCallMap = _impl->_rpcServer->GetRPCCallMap();
        _impl->_rpcNameMap.clear();
        for (ANetRPCServer::RPCCallMap::const_iterator it = rpcCallMap.begin(); it != rpcCallMap.end(); ++it) {
            string key = "/" + it->second.first.second->GetDescriptor()->name() + "/" + it->second.second->name();
            _impl->_rpcNameMap[key] = it->second;
            _impl->SetServiceThreadPool(it->second.first.second, _impl->GetThreadPool(DEFAULT_HTTP_TREAHDPOOL_NAME));
        }
        for (auto it = _aliasMap.begin(); it != _aliasMap.end(); it++) {
            const string &to = it->first;
            const string &from = it->second;
            auto rit = _impl->_rpcNameMap.find(from);
            if (rit == _impl->_rpcNameMap.end()) {
                HTTP_ARPC_LOG(WARN, "add alias from[%s] to[%s] failed", from.c_str(), to.c_str());
                continue;
            }
            _impl->_rpcNameMap[to] = rit->second;
        }
    }
    refreshAddMethod();
}

bool HTTPRPCServer::addAlias(const AliasMap &aliasMap) {
    {
        ScopedWriteLock lock(_methodMapMutex);
        _aliasMap.insert(aliasMap.begin(), aliasMap.end());
    }
    registerService();
    return true;
}

std::vector<std::string> HTTPRPCServer::getRPCNames() {
    ScopedReadLock lock(_methodMapMutex);
    RPCNameMap &rpcMap = _impl->_rpcNameMap;
    vector<string> names;
    for (RPCNameMap::const_iterator it = rpcMap.begin(); it != rpcMap.end(); ++it) {
        names.push_back(it->first);
    }
    return names;
}

bool HTTPRPCServer::StartPrivateTransport() { return _impl->_anetApp.StartPrivateTransport(); }

bool HTTPRPCServer::StopPrivateTransport() { return _impl->_anetApp.StopPrivateTransport(); }

bool HTTPRPCServer::StartThreads() { return _impl->GetThreadPool(DEFAULT_HTTP_TREAHDPOOL_NAME)->start(); }

bool HTTPRPCServer::Listen(const string &spec, int timeout, int maxIdleTime, int backlog) {
    IOComponent *ioComponent = _impl->_anetApp.Listen(spec, &_impl->_serverAdapter, timeout, maxIdleTime, backlog);
    if (ioComponent == NULL) {
        HTTP_ARPC_LOG(ERROR, "http arpc listen failed");
        return false;
    }
    Socket *socket = ioComponent->getSocket();
    if (socket != nullptr) {
        _listenPort = socket->getPort();
    }
    autil::ScopedLock lock(_impl->_ioComponentMutex);
    _impl->_ioComponentList.push_back(ioComponent);

    return true;
}

bool HTTPRPCServer::PushItem(RPCService *service, HTTPRPCServerWorkItem *workItem) {

    autil::ThreadPoolBasePtr threadPoolPtr = _impl->GetServiceThreadPool(service);
    if (!threadPoolPtr) {
        HTTP_ARPC_LOG(ERROR, "can't find threadpool");
        return false;
    }
    autil::ThreadPool::ERROR_TYPE code = threadPoolPtr->pushWorkItem(workItem, false);
    if (code != autil::ThreadPool::ERROR_NONE) {
        HTTP_ARPC_LOG(ERROR, "drop work item");
        delete workItem;
        return false;
    }
    return true;
}

ServiceMethodPair HTTPRPCServer::getMethod(const std::string &serviceAndMethod) {
    ScopedReadLock lock(_methodMapMutex);
    RPCNameMap::const_iterator it = _impl->_rpcNameMap.find(serviceAndMethod);
    if (it != _impl->_rpcNameMap.end()) {
        return it->second;
    } else {
        return RPCServer::ServiceMethodPair();
    }
}

void HTTPRPCServer::Close() {
    {
        autil::ScopedLock lock(_impl->_ioComponentMutex);
        list<IOComponent *>::iterator it = _impl->_ioComponentList.begin();
        for (; it != _impl->_ioComponentList.end(); ++it) {
            (*it)->close();
            (*it)->subRef();
        }
        _impl->_ioComponentList.clear();
    }
}

int HTTPRPCServer::getPort() { return _listenPort; }
void HTTPRPCServer::setProtoJsonizer(const ProtoJsonizerPtr &protoJsonizer) {
    _impl->_serverAdapter.setProtoJsonizer(protoJsonizer);
}

ProtoJsonizerPtr HTTPRPCServer::getProtoJsonizer() { return _impl->_serverAdapter.getProtoJsonizer(); }

bool HTTPRPCServer::setProtoJsonizer(RPCService *service,
                                     const std::string &method,
                                     const ProtoJsonizerPtr &protoJsonizer) {
    return _impl->_serverAdapter.setProtoJsonizer(service, method, protoJsonizer);
}

bool HTTPRPCServer::needDecodeUri() { return _impl->_decodeUri; }

bool HTTPRPCServer::haCompatible() { return _impl->_haCompatible; }

bool HTTPRPCServer::addMethod(const string &methodName, ServiceMethodPair &methodPair) {
    return addMethod(
        methodName,
        methodPair,
        ThreadPoolDescriptor(DEFAULT_HTTP_TREAHDPOOL_NAME, _impl->_defaultThreadNum, _impl->_defaultQueueSize));
}

bool HTTPRPCServer::addMethod(const string &methodName,
                              const ServiceMethodPair &methodPair,
                              const ThreadPoolDescriptor &threadPoolDescriptor) {
    _addMethods[methodName] = std::make_pair(methodPair, threadPoolDescriptor);
    return refreshAddMethod();
}

bool HTTPRPCServer::refreshAddMethod() {
    bool ret = true;
    for (const auto &pair : _addMethods) {
        const auto &methodName = pair.first;
        const auto &methodPair = pair.second.first;
        const auto &threadPoolDescriptor = pair.second.second;
        if (!_impl->addAndStartThreadPool(
                threadPoolDescriptor.threadPoolName, threadPoolDescriptor.threadNum, threadPoolDescriptor.queueSize)) {
            HTTP_ARPC_LOG(ERROR, "start thread pool failed");
            ret = false;
            continue;
        }
        {
            ScopedWriteLock lock(_methodMapMutex);
            RPCNameMap::const_iterator it = _impl->_rpcNameMap.find(methodName);
            if (it != _impl->_rpcNameMap.end()) {
                HTTP_ARPC_LOG(ERROR, "%s already exist", methodName.c_str());
                ret = false;
                continue;
            }
            _impl->_rpcNameMap[methodName] = methodPair;
        }
        _impl->SetServiceThreadPool(methodPair.first.second, _impl->GetThreadPool(threadPoolDescriptor.threadPoolName));
    }
    return ret;
}

bool HTTPRPCServer::hasMethod(const string &methodName) {
    ScopedReadLock lock(_methodMapMutex);
    RPCNameMap::const_iterator it = _impl->_rpcNameMap.find(methodName);
    return it != _impl->_rpcNameMap.end();
}

size_t HTTPRPCServer::getQueueSize(const std::string &name) { return _impl->getQueueSize(name); }

size_t HTTPRPCServer::getQueueSize() { return getQueueSize(DEFAULT_HTTP_TREAHDPOOL_NAME); }

size_t HTTPRPCServer::getItemCount(const std::string &name) { return _impl->getItemCount(name); }

size_t HTTPRPCServer::getItemCount() { return getItemCount(DEFAULT_HTTP_TREAHDPOOL_NAME); }

autil::ThreadPoolBasePtr HTTPRPCServer::getThreadPool(const std::string &name) {
    if (name.empty()) {
        return _impl->GetThreadPool(DEFAULT_HTTP_TREAHDPOOL_NAME);
    } else {
        return _impl->GetThreadPool(name);
    }
}

} // namespace http_arpc
