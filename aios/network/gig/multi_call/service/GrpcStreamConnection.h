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
#ifndef ISEARCH_MULTI_CALL_GRPCSTREAMCONNECTION_H
#define ISEARCH_MULTI_CALL_GRPCSTREAMCONNECTION_H

#include "aios/network/gig/multi_call/grpc/CompletionQueueStatus.h"
#include "aios/network/gig/multi_call/service/GrpcConnection.h"
#include <grpc++/generic/generic_stub.h>

namespace multi_call {

class GigStreamRequest;
class GrpcClientStreamHandler;

class GrpcStreamConnection : public GrpcConnection {
public:
    GrpcStreamConnection(const GrpcClientWorkerPtr &grpcWorker, const std::string &spec,
                         const std::shared_ptr<grpc::ChannelCredentials> &channelCredentials,
                         const std::shared_ptr<grpc::ChannelArguments> &channelArgs,
                         const ObjectPoolPtr &objectPool);
    ~GrpcStreamConnection();

private:
    GrpcStreamConnection(const GrpcStreamConnection &);
    GrpcStreamConnection &operator=(const GrpcStreamConnection &);

public:
    void post(const RequestPtr &request, const CallBackPtr &callBack) override;
private:
    std::shared_ptr<GrpcClientStreamHandler> createGrpcHandler(
            const std::string &bizName,
            const GigStreamRequest *request,
            PartIdTy partId);
    bool createGrpcStream(int64_t handlerId, const GigStreamRequest *request,
                          grpc::ClientContext *clientContext, CompletionQueueStatusPtr &cqs,
                          grpc::GenericClientAsyncReaderWriter *&stream);
private:
    ObjectPoolPtr _objectPool;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcStreamConnection);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCSTREAMCONNECTION_H
