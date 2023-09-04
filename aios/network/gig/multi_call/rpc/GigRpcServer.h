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
#ifndef ISEARCH_MULTI_CALL_GIGRPCSERVER_H
#define ISEARCH_MULTI_CALL_GIGRPCSERVER_H

#include "aios/network/arpc/arpc/RPCServer.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "aios/network/gig/multi_call/rpc/ServerBizTopoInfo.h"
#include "aios/network/gig/multi_call/stream/GigServerStreamCreator.h"
#include "autil/Lock.h"
#include "autil/LockFreeThreadPool.h"

namespace arpc {
class RPCServer;
class ANetRPCServer;
} // namespace arpc

namespace http_arpc {
class HTTPRPCServer;
}

namespace anet {
class Transport;
}

namespace multi_call {

struct BizTopoInfo;
class GrpcServerWorker;
class ServiceWrapper;
class SearchService;
struct GigRpcServerConfig;
struct GrpcServerDescription;
class GigAgent;
class HeartbeatServerManager;
class RdmaArpcServerWapper;

MULTI_CALL_TYPEDEF_PTR(SearchService);
MULTI_CALL_TYPEDEF_PTR(GigAgent);

class GigRpcServer
{
public:
    GigRpcServer();
    GigRpcServer(arpc::ANetRPCServer *arpcServer); // no auto delete arpc server
    ~GigRpcServer();

private:
    GigRpcServer(const GigRpcServer &);
    GigRpcServer &operator=(const GigRpcServer &);

public:
    bool init(const GigRpcServerConfig &serverConfig = {});
    void release();
    bool startArpcServer(ArpcServerDescription &desc);
    bool startHttpArpcServer(HttpArpcServerDescription &desc);
    bool startGrpcServer(const GrpcServerDescription &desc);
    bool initGrpcServer(const GrpcServerDescription &desc) {
        return startGrpcServer(desc);
    }
    bool startRdmaArpcServer(RdmaArpcServerDescription &desc);
    void setTcpPort(int32_t tcpPort);

public:
    bool registerArpcService(
        google::protobuf::Service *rpcService, const CompatibleFieldInfo &compatibleInfo,
        const arpc::ThreadPoolDescriptor &threadPoolDescriptor = arpc::ThreadPoolDescriptor());
    bool registerArpcService(google::protobuf::Service *rpcService,
                             const CompatibleFieldInfo &compatibleInfo,
                             const arpc::ThreadPoolDescriptor &threadPoolDescriptor,
                             const std::map<std::string, std::string> &aliasMap);
    bool registerArpcService(const std::shared_ptr<google::protobuf::Service> &rpcService,
                             const std::string &methodName,
                             const CompatibleFieldInfo &compatibleInfo,
                             const arpc::ThreadPoolDescriptor &threadPoolDescriptor);
    void unRegisterArpcService(const std::shared_ptr<google::protobuf::Service> &rpcService);
    bool registerGrpcService(const std::string &methodName, google::protobuf::Message *request,
                             google::protobuf::Message *response,
                             const CompatibleFieldInfo &compatibleInfo, const GigRpcMethod &method);
    bool registerGrpcStreamService(const GigServerStreamCreatorPtr &creator);
    bool unRegisterGrpcStreamService(const GigServerStreamCreatorPtr &creator);
    bool registerArpcService(google::protobuf::Service *rpcService,
                             const CompatibleFieldInfo &compatibleInfo,
                             const autil::ThreadPoolBasePtr &pool);

public:
    void setSearchService(const SearchServicePtr &searchService);
    SearchServicePtr getSearchService();
    GigAgentPtr &getAgent();
    void startAgent();
    void stopAgent();
    arpc::ANetRPCServer *getArpcServer() const;
    http_arpc::HTTPRPCServer *getHttpArpcServer() const;
    std::shared_ptr<arpc::RPCServer> getRdmaRPCServer() const;

public:
    bool hasGrpc() const;
    int32_t getGrpcPort() const;
    bool waitUntilNoQuery(int64_t noQueryTimeInSecond,
                          int64_t timeoutInSecond = std::numeric_limits<int64_t>::max());

public:
    // topo publish and heartbeat
    bool publish(const ServerBizTopoInfo &info, SignatureTy &signature);
    bool publish(const std::vector<ServerBizTopoInfo> &infoVec,
                 std::vector<SignatureTy> &signatureVec);
    bool publishGroup(PublishGroupTy group, const std::vector<ServerBizTopoInfo> &infoVec,
                      std::vector<SignatureTy> &signatureVec);
    bool publishGroup(PublishGroupTy group, const std::vector<BizTopoInfo> &infoVec,
                      std::vector<SignatureTy> &signatureVec);
    bool updateVolatileInfo(SignatureTy signature, const BizVolatileInfo &info);
    bool unpublish(SignatureTy signature);
    bool unpublish(const std::vector<SignatureTy> &signatureVec);
    std::vector<ServerBizTopoInfo> getPublishInfos() const;
    std::string getTopoInfoStr() const;

private:
    std::shared_ptr<GrpcServerWorker> createGrpcServer(const GrpcServerDescription &desc);
    void stopHttpArpcServer();
    void stopArpcServer();
    void stopGrpcServer();
    void stopRdmaArpcServer();
    template <typename ThreadPoolRep>
    bool registerArpcService(google::protobuf::Service *rpcService,
                             const CompatibleFieldInfo &compatibleInfo,
                             const ThreadPoolRep &poolRep);
    bool registerHeartbeat();
    void stopHeartbeat();
    Spec getRpcSpec();

private:
    std::shared_ptr<arpc::ANetRPCServer> _arpcServer;
    std::shared_ptr<anet::Transport> _arpcServerTransport;
    ArpcServerDescription _arpcDesc;
    bool _autoDelete;

    std::shared_ptr<http_arpc::HTTPRPCServer> _httpArpcServer;
    std::shared_ptr<anet::Transport> _httpArpcServerTransport;
    HttpArpcServerDescription _httpArpcDesc;

    std::shared_ptr<GrpcServerWorker> _grpcWorker;

    std::shared_ptr<RdmaArpcServerWapper> _rdmaArpcServerWrapper;

    autil::SpinLock _spinLock;
    SearchServicePtr _searchService;
    GigAgentPtr _agent;
    std::set<std::shared_ptr<ServiceWrapper>> _wrappers;
    std::shared_ptr<HeartbeatServerManager> _heartbeatManager;
    GigServerStreamCreatorPtr _heartbeatCreator;
    int32_t _tcpPort;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigRpcServer);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGRPCSERVER_H
