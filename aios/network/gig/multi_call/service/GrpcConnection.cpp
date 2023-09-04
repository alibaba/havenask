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
#include "aios/network/gig/multi_call/service/GrpcConnection.h"

#include <grpc++/generic/generic_stub.h>
#include <unistd.h>

#include "aios/network/gig/multi_call/grpc/client/GrpcClientClosure.h"
#include "aios/network/gig/multi_call/interface/GrpcRequest.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcConnection);

GrpcConnection::GrpcConnection(const GrpcClientWorkerPtr &grpcWorker, const std::string &spec,
                               const std::shared_ptr<grpc::ChannelCredentials> &channelCredentials,
                               const std::shared_ptr<grpc::ChannelArguments> &channelArgs)
    : Connection(spec, MC_PROTOCOL_GRPC, 0)
    , _grpcWorker(grpcWorker)
    , _channelCredentials(channelCredentials)
    , _channelArgs(channelArgs) {
}

GrpcConnection::GrpcConnection(const GrpcClientWorkerPtr &grpcWorker, const std::string &spec,
                               const std::shared_ptr<grpc::ChannelCredentials> &channelCredentials,
                               const std::shared_ptr<grpc::ChannelArguments> &channelArgs,
                               ProtocolType type)
    : Connection(spec, type, 0)
    , _grpcWorker(grpcWorker)
    , _channelCredentials(channelCredentials)
    , _channelArgs(channelArgs) {
}

GrpcConnection::~GrpcConnection() {
}

GrpcChannelPtr GrpcConnection::getChannel() {
    {
        ScopedReadWriteLock lock(_channelLock, 'r');
        if (_channel) {
            return _channel;
        }
    }
    {
        ScopedReadWriteLock lock(_channelLock, 'w');
        if (_channel) {
            return _channel;
        }
        if (!_channelArgs) {
            _channel = grpc::CreateChannel(_spec, _channelCredentials);
        } else {
            _channel = grpc::CreateCustomChannel(_spec, _channelCredentials, *_channelArgs);
        }
        return _channel;
    }
}

void GrpcConnection::post(const RequestPtr &request, const CallBackPtr &callBack) {
    const auto &resource = callBack->getResource();
    auto grpcRequest = dynamic_cast<GrpcRequest *>(request.get());
    if (!grpcRequest) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_REQUEST, string(), string());
        return;
    }
    auto channel = getChannel();
    if (!channel) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_CONNECTION, string(), string());
        return;
    }

    grpcRequest->setRequestType(resource->getRequestType());
    auto message = grpcRequest->getMessage();
    if (!message) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE, string(), string());
        return;
    }
    auto closure = new GrpcClientClosure(message, callBack);
    auto clientContext = closure->getClientContext();
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(resource->getTimeout());
    clientContext->set_deadline(deadline);

    clientContext->AddMetadata(GIG_GRPC_REQUEST_INFO_KEY, grpcRequest->getAgentQueryInfo());
    grpc::GenericStub stub(channel);
    auto cqs = _grpcWorker->getCompletionQueue(_allocId);
    assert(cqs);
    callBack->rpcBegin();
    std::unique_ptr<grpc::GenericClientAsyncResponseReader> rpc(stub.PrepareUnaryCall(
        clientContext, grpcRequest->getMethodName(), closure->getRequestBuf(), cqs->cq.get()));
    rpc->StartCall();
    rpc->Finish(closure->getResponseBuf(), &closure->getStatus(), closure);
}

} // namespace multi_call
