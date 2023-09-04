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
#ifndef ISEARCH_MULTI_CALL_GRPCSERVERSTREAMHANDLER_H
#define ISEARCH_MULTI_CALL_GRPCSERVERSTREAMHANDLER_H

#include <grpc++/generic/async_generic_service.h>

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/grpc/server/GrpcServerWorker.h"
#include "aios/network/gig/multi_call/proto/GigStreamHeader.pb.h"
#include "aios/network/gig/multi_call/stream/GigStreamHandlerBase.h"

namespace multi_call {

class QueryInfo;
class GrpcServerWorker;

class GrpcServerStreamHandler : public GigStreamHandlerBase
{
public:
    GrpcServerStreamHandler(GrpcServerWorker *worker, const ServerCompletionQueueStatusPtr &cqs);
    ~GrpcServerStreamHandler();

private:
    GrpcServerStreamHandler(const GrpcServerStreamHandler &);
    GrpcServerStreamHandler &operator=(const GrpcServerStreamHandler &);

public:
    bool init() override;

public:
    QueryInfoPtr getQueryInfo() const;
    std::string getPeer() const;

private:
    void triggerNew();
    void initCallback(bool ok) override;
    bool receiveBeginQuery();
    bool receiveInitQuery();
    bool getServerContextMetaValue(const std::string &key, std::string &value) const;
    GigStreamClosureBase *doSend(bool sendEof, grpc::ByteBuffer *message,
                                 GigStreamClosureBase *closure, bool &ok) override;
    GigStreamClosureBase *receiveNext() override;
    bool ignoreReceiveError() override;
    GigStreamClosureBase *finish() override;
    void doFinishCallback(bool ok) override;
    void clean() override;
    int64_t getLatency() const;
    void setQueryInfo(const std::shared_ptr<QueryInfo> &queryInfo);
    std::shared_ptr<QueryInfo> stealQueryInfo();

private:
    // receive
    GrpcServerWorker *_worker;
    ServerCompletionQueueStatusPtr _cqs;
    grpc::GenericServerContext _serverContext;
    grpc::GenericServerAsyncReaderWriter _grpcStream;
    bool _triggered;
    bool _finished;
    int64_t _beginTime;
    mutable autil::ThreadMutex _queryInfoLock;
    std::shared_ptr<QueryInfo> _queryInfo;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcServerStreamHandler);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCSERVERSTREAMHANDLER_H
