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
#ifndef HTTP_ARPC_HTTPRPCSERVER_H
#define HTTP_ARPC_HTTPRPCSERVER_H

#include "aios/network/http_arpc/ProtoJsonizer.h"
#include <string>
#include <list>
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include <unordered_map>
#include "autil/Lock.h"

namespace anet {
class Transport;
}

namespace arpc {
class ANetRPCServer;
}

namespace http_arpc {

/**
 * Default timeout to break idle connections.
 */
const int TIMEOUT = 5000;

/**
 * Default time span to break idle connections.
 */
const int MAX_IDLE_TIME = 900000; //15 minutes

/**
 * Default backlog for server's listen socket
 */
const int LISTEN_BACKLOG = 256;

class HTTPRPCServerWorkItem;
class HTTPRPCServerImpl;

typedef std::pair<RPCService*, RPCMethodDescriptor*> ServiceMethodPair;
typedef std::unordered_map<std::string, ServiceMethodPair> RPCNameMap;
typedef std::map<std::string, std::string> AliasMap;

class HTTPRPCServer
{
public:
    HTTPRPCServer(arpc::ANetRPCServer *rpcServer,
                  anet::Transport *transport = NULL,
                  size_t threadNum = 1,
                  size_t queueSize = 50);

    HTTPRPCServer(arpc::ANetRPCServer *rpcServer,
                  anet::Transport *transport,
                  size_t threadNum,
                  size_t queueSize,
                  bool decodeUri,
                  bool haCompatible);
    virtual ~HTTPRPCServer();

public:
    virtual bool StartThreads();
    virtual bool StartPrivateTransport();
    virtual bool StopPrivateTransport();
    virtual bool Listen(const std::string &spec,
                int timeout = TIMEOUT,
                int maxIdleTime = MAX_IDLE_TIME,
                int backlog = LISTEN_BACKLOG);
    virtual void Close();
    int getPort();
    void setProtoJsonizer(const ProtoJsonizerPtr& protoJsonizer);
    ProtoJsonizerPtr getProtoJsonizer();
    bool needDecodeUri();
    bool haCompatible();
public:
    bool PushItem(RPCService* service, HTTPRPCServerWorkItem *workItem);
    void registerService();
    bool addAlias(const AliasMap &aliasMap);
    std::vector<std::string> getRPCNames();
    ServiceMethodPair getMethod(const std::string &serviceAndMethod);
    bool addMethod(const std::string &methodName, ServiceMethodPair& methodPair);
    bool addMethod(const std::string &methodName, const ServiceMethodPair& methodPair,
                   const arpc::ThreadPoolDescriptor &threadPoolDescriptor);
    bool hasMethod(const std::string &methodName);

    size_t getQueueSize();
    size_t getQueueSize(const std::string &name);
    size_t getItemCount();
    size_t getItemCount(const std::string &name);

    autil::ThreadPoolBasePtr getThreadPool(const std::string &name);
private:
    HTTPRPCServerImpl *_impl = nullptr;
    autil::ReadWriteLock _methodMapMutex;
    int _listenPort = -1;
};

}

#endif //HTTP_ARPC_HTTPRPCSERVER_
