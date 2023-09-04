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
#include "aios/network/gig/multi_call/service/GrpcStreamConnection.h"

#include "aios/network/gig/multi_call/grpc/client/GrpcClientStreamHandler.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/stream/GigClientStream.h"
#include "aios/network/gig/multi_call/stream/GigStreamRequest.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcStreamConnection);

GrpcStreamConnection::GrpcStreamConnection(
    const GrpcClientWorkerPtr &grpcWorker, const std::string &spec,
    const std::shared_ptr<grpc::ChannelCredentials> &channelCredentials,
    const std::shared_ptr<grpc::ChannelArguments> &channelArgs, const ObjectPoolPtr &objectPool)
    : GrpcConnection(grpcWorker, spec, channelCredentials, channelArgs, MC_PROTOCOL_GRPC_STREAM)
    , _objectPool(objectPool) {
    AUTIL_LOG(INFO, "create grpc stream connection spec [%s] allocId [%lu]", _spec.c_str(),
              _allocId);
}

GrpcStreamConnection::~GrpcStreamConnection() {
}

void GrpcStreamConnection::post(const RequestPtr &request, const CallBackPtr &callBack) {
    auto streamRequest = dynamic_pointer_cast<GigStreamRequest>(request);
    assert(streamRequest);
    const auto &resource = callBack->getResource();
    auto partId = resource->getPartId();
    auto handler = createGrpcHandler(resource->getBizName(), streamRequest.get(), partId);
    if (!handler) {
        AUTIL_LOG(DEBUG, "create grpc client handler failed");
        return;
    }
    handler->setRequestInfo(streamRequest.get(), callBack);
    streamRequest->addHandler(resource->getRequestType(), handler, callBack->isRetry());
}

GrpcClientStreamHandlerPtr GrpcStreamConnection::createGrpcHandler(const std::string &bizName,
                                                                   const GigStreamRequest *request,
                                                                   PartIdTy partId) {
    auto clientStream = request->getStream();
    if (!clientStream) {
        AUTIL_LOG(DEBUG, "null client stream");
        return nullptr;
    }
    bool corked = true;
    GrpcClientStreamHandlerPtr streamHandler(
        new GrpcClientStreamHandler(clientStream, partId, bizName, getSpec(), corked));
    streamHandler->setObjectPool(_objectPool);
    // streamHandler->setCallBackThreadPool(_callBackThreadPool);
    grpc::GenericClientAsyncReaderWriter *grpcStream = nullptr;
    CompletionQueueStatusPtr cqs;
    if (!createGrpcStream(streamHandler->getHandlerId(), request, corked,
                          streamHandler->getClientContext(), cqs, grpcStream)) {
        return GrpcClientStreamHandlerPtr();
    }
    streamHandler->setGrpcReaderWriter(cqs, grpcStream);
    return streamHandler;
}

bool GrpcStreamConnection::createGrpcStream(int64_t handlerId, const GigStreamRequest *request,
                                            bool corked, grpc::ClientContext *clientContext,
                                            CompletionQueueStatusPtr &cqs,
                                            grpc::GenericClientAsyncReaderWriter *&stream) {
    auto channel = getChannel();
    if (!channel) {
        AUTIL_LOG(DEBUG, "create grpc stream failed, null channel");
        return false;
    }
    auto timeout = request->getTimeout();
    if (timeout != INFINITE_TIMEOUT) {
        auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
        clientContext->set_deadline(deadline);
    }
    if (corked) {
        clientContext->set_initial_metadata_corked(true);
    }
    clientContext->AddMetadata(GIG_GRPC_REQUEST_INFO_KEY, request->getAgentQueryInfo());
    clientContext->AddMetadata(GIG_GRPC_TYPE_KEY, GIG_GRPC_TYPE_STREAM);
    clientContext->AddMetadata(GIG_GRPC_HANDLER_ID_KEY, autil::StringUtil::toString(handlerId));
    grpc::GenericStub stub(channel);
    cqs = _grpcWorker->getCompletionQueue(_allocId);
    assert(cqs);
    assert(cqs->cq);
    stream = stub.PrepareCall(clientContext, request->getMethodName(), cqs->cq.get()).release();
    return cqs != nullptr && stream != nullptr;
}

} // namespace multi_call
