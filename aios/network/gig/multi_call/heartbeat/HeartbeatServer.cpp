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
#include "aios/network/gig/multi_call/heartbeat/HeartbeatServer.h"
#include "aios/network/gig/multi_call/agent/GigAgent.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/grpc/server/GrpcServerWorker.h"
#include <string>

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatServer);

HeartbeatServer::HeartbeatServer(arpc::ANetRPCServer *arpcServer,
                                 const GigAgentPtr &agent,
                                 const MetaManagerServerPtr &metaManagerServer)
    : _arpcServer(arpcServer), _agent(agent),
      _metaServerPtr(metaManagerServer) {}

HeartbeatServer::~HeartbeatServer() {}

bool HeartbeatServer::init(const GigRpcServerConfig &serverConfig) {
    if (!_arpcServer) {
        AUTIL_LOG(ERROR, "init heartbeat server faild: _arpcServer is null");
        return false;
    }
    if (!registerArpcHeartbeat(serverConfig.heartbeatThreadNum,
                               serverConfig.heartbeatQueueSize)) {
        return false;
    }

    if (!_metaServerPtr->init()) {
        AUTIL_LOG(ERROR, "init meta server manager failed");
        return false;
    }
    return true;
}

bool HeartbeatServer::registerArpcHeartbeat(size_t threadNum,
                                            size_t queueSize) {
    arpc::ThreadPoolDescriptor workerTpd;
    workerTpd.threadNum = threadNum;
    workerTpd.queueSize = queueSize;
    if (!_arpcServer->RegisterService(this, workerTpd)) {
        AUTIL_LOG(ERROR, "register gig arpc heartbeat service failed");
        return false;
    }
    AUTIL_LOG(INFO, "register gig arpc heartbeat success");
    return true;
}

bool HeartbeatServer::registerGrpcHeartbeat(
    std::shared_ptr<GrpcServerWorker> &grpcWorker) {
    if (!grpcWorker) {
        AUTIL_LOG(ERROR, "grpc worker is null");
        return false;
    }
    GigRpcMethodArgPtr arg(new GigRpcMethodArg());
    arg->request = new ::multi_call::HeartbeatRequest();
    arg->response = new ::multi_call::HeartbeatResponse();
    arg->method =
        std::bind(&HeartbeatServer::gig_heartbeat_grpc, this, placeholders::_1,
                  placeholders::_2, placeholders::_3, placeholders::_4);
    arg->isHeartbeatMethod =
        true; // heartbeat request donot need to create query info

    if (!grpcWorker->addMethod(GIG_GRPC_HEARTBEAT_METHOD_NAME, arg)) {
        AUTIL_LOG(ERROR, "register gig grpc heartbeat failed");
        return false;
    }
    AUTIL_LOG(INFO, "register gig grpc heartbeat success");
    return true;
}

MetaManagerServerPtr &HeartbeatServer::getMetaManagerServer() {
    return _metaServerPtr;
}

void HeartbeatServer::fillMetaResponse(
    const ::multi_call::HeartbeatRequest &request, ::multi_call::Meta *meta) {
    _metaServerPtr->fillMeta(request, meta);
}

void HeartbeatServer::gig_heartbeat(
    ::google::protobuf::RpcController *controller,
    const ::multi_call::HeartbeatRequest *request,
    ::multi_call::HeartbeatResponse *response,
    ::google::protobuf::Closure *done) {
    response->set_stopped(_agent->isStopped());
    fillMetaResponse(*request, response->mutable_meta());

    done->Run();
}

void HeartbeatServer::gig_heartbeat_grpc(
    ::google::protobuf::RpcController *controller,
    google::protobuf::Message *request, google::protobuf::Message *response,
    ::google::protobuf::Closure *done) {
    auto requestTyped = dynamic_cast<const HeartbeatRequest *>(request);
    auto responseTyped = dynamic_cast<HeartbeatResponse *>(response);
    if (!requestTyped || !responseTyped) {
        controller->SetFailed("invalid request or response");
        done->Run();
    } else {
        gig_heartbeat(controller, requestTyped, responseTyped, done);
    }
}

} // namespace multi_call
