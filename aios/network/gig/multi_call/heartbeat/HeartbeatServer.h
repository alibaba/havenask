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
#ifndef ISEARCH_MULTI_CALL_HEARTBEATSERVER_H
#define ISEARCH_MULTI_CALL_HEARTBEATSERVER_H

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/heartbeat/MetaManagerServer.h"
#include "aios/network/gig/multi_call/proto/Heartbeat.pb.h"

namespace multi_call {

class GrpcServerWorker;
struct GigRpcServerConfig;
class GigAgent;
MULTI_CALL_TYPEDEF_PTR(GigAgent);

class HeartbeatServer : public HeartbeatService {
public:
    HeartbeatServer(arpc::ANetRPCServer *arpcServer, const GigAgentPtr &agent,
                    const MetaManagerServerPtr &MetaManagerServer);
    ~HeartbeatServer();

private:
    HeartbeatServer(const HeartbeatServer &);
    HeartbeatServer &operator=(const HeartbeatServer &);

public:
    bool init(const GigRpcServerConfig &serverConfig);
    void fillMetaResponse(const ::multi_call::HeartbeatRequest &request,
                          ::multi_call::Meta *meta);
    MetaManagerServerPtr &getMetaManagerServer();
    bool registerGrpcHeartbeat(std::shared_ptr<GrpcServerWorker> &grpcWorker);

private:
    bool registerArpcHeartbeat(size_t threadNum, size_t queueSize);

public:
    void gig_heartbeat(::google::protobuf::RpcController *controller,
                       const ::multi_call::HeartbeatRequest *request,
                       ::multi_call::HeartbeatResponse *response,
                       ::google::protobuf::Closure *done) override;
    void gig_heartbeat_grpc(::google::protobuf::RpcController *controller,
                            google::protobuf::Message *request,
                            google::protobuf::Message *response,
                            ::google::protobuf::Closure *done);

private:
    arpc::ANetRPCServer *_arpcServer;
    GigAgentPtr _agent;
    MetaManagerServerPtr _metaServerPtr;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatServer);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_HEARTBEATSERVER_H
