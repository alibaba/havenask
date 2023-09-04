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
#ifndef ISEARCH_MULTI_CALL_GRPCCALLDATA_H
#define ISEARCH_MULTI_CALL_GRPCCALLDATA_H

#include <grpc++/generic/async_generic_service.h>
#include <grpc++/grpc++.h>
#include <grpc++/impl/codegen/byte_buffer.h>

#include "aios/network/gig/multi_call/grpc/server/GigGrpcClosure.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "autil/Lock.h"

namespace multi_call {

typedef std::shared_ptr<grpc::ServerCompletionQueue> ServerCompletionQueuePtr;

struct ServerCompletionQueueStatus {
    ServerCompletionQueueStatus(const ServerCompletionQueuePtr &cq_) : stopped(false), cq(cq_) {
    }
    volatile bool stopped;
    autil::ReadWriteLock enqueueLock;
    ServerCompletionQueuePtr cq;
};

MULTI_CALL_TYPEDEF_PTR(ServerCompletionQueueStatus);

class GrpcServerWorker;
struct GigRpcMethodArg;

struct GrpcCallData {
    enum CallStatus { CREATE, READ, PROCESS, FINISH };

    GrpcCallData(GrpcServerWorker *worker, ServerCompletionQueueStatus *cqs,
                 const SearchServicePtr &searchService);
    ~GrpcCallData();

private:
    GrpcCallData();

public:
    void process();
    void trigger();

private:
    void beginQuery();
    void call();
    void initQueryInfo(const std::string &infoStr);
    void initQueryInfoByCompatibleField(google::protobuf::Message *request,
                                        const std::shared_ptr<GigRpcMethodArg> &arg);
    void initQuerySession(google::protobuf::Message *request,
                          const std::shared_ptr<GigRpcMethodArg> &arg);

public:
    std::shared_ptr<google::protobuf::Arena> arena;
    CallStatus status;
    bool triggered;
    int64_t beginTime;
    GrpcServerWorker *worker;
    ServerCompletionQueueStatus *cqs;
    grpc::GenericServerContext serverContext;
    grpc::GenericServerAsyncReaderWriter stream;
    grpc::ByteBuffer receiveBuffer;
    grpc::ByteBuffer sendBuffer;
    google::protobuf::Message *request;
    google::protobuf::Message *response;
    GigGrpcClosure *closure;
    SearchServicePtr searchService;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCCALLDATA_H
