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
#ifndef ISEARCH_MULTI_CALL_GRPCCLIENTSTREAMHANDLER_H
#define ISEARCH_MULTI_CALL_GRPCCLIENTSTREAMHANDLER_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/grpc/CompletionQueueStatus.h"
#include "aios/network/gig/multi_call/stream/GigStreamHandlerBase.h"
#include "aios/network/gig/multi_call/stream/GigStreamMessage.h"
#include "aios/network/opentelemetry/core/Span.h"
#include <grpc++/generic/async_generic_service.h>
#include <grpc++/generic/generic_stub.h>

// #define SET_DEBUG

namespace multi_call {

class GigStreamBase;
class GigStreamRequest;
struct SingleCallInfo;
class CallBack;
class SearchServiceProvider;
struct CompletionQueueStatus;

class GrpcClientStreamHandler : public GigStreamHandlerBase {
public:
    GrpcClientStreamHandler(const std::shared_ptr<GigStreamBase> &stream, PartIdTy partId,
                            const std::string &bizName, const std::string &spec);
    ~GrpcClientStreamHandler();

private:
    GrpcClientStreamHandler(const GrpcClientStreamHandler &);
    GrpcClientStreamHandler &operator=(const GrpcClientStreamHandler &);

public:
    grpc::ClientContext *getClientContext();
    void setGrpcReaderWriter(const CompletionQueueStatusPtr &cqs,
                             grpc::GenericClientAsyncReaderWriter *readerWriter);
    void setRequestInfo(GigStreamRequest *request, const std::shared_ptr<CallBack> &callBack);
    void setCallInfo(SingleCallInfo *callInfo);
    GigStreamRpcInfo snapshotStreamRpcInfo() const;

    void setObjectPool(const ObjectPoolPtr &objectPool) {
#ifdef SET_DEBUG
        _objectPool = objectPool;
        if (_objectPool) {
            autil::ScopedLock lock(_objectPool->lock);
            _objectPool->set.insert(this);
        }
#endif
    }
public:
    bool init() override;
    void abort();
private:
    void initCallback(bool ok) override;
    GigStreamClosureBase *doSend(bool sendEof, grpc::ByteBuffer *message,
                                 GigStreamClosureBase *closure) override;
    void tryCancel() override;
    GigStreamClosureBase *receiveNext() override;
    bool doStreamReceive(const std::shared_ptr<GigStreamBase> &stream,
                         const GigStreamMessage &message) override;
    void doStreamReceiveCancel(const std::shared_ptr<GigStreamBase> &stream,
                               const GigStreamMessage &message,
                               MultiCallErrorCode ec) override;
    bool ignoreReceiveError() override;
    GigStreamClosureBase *finish() override;
    void doFinishCallback(bool ok) override;
    void initChildSpan();
    void clean() override;
    void updateClockDrift();
private:
    // receive
    std::string _bizName;
    std::string _spec;
    grpc::ClientContext _context;
    CompletionQueueStatusPtr _cqs;
    grpc::GenericClientAsyncReaderWriter *_grpcReaderWriter;
    GigStreamRequest *_request;
    SingleCallInfo *_callInfo;
    std::shared_ptr<CallBack> _callBack;
    std::shared_ptr<SearchServiceProvider> _provider;
    std::atomic<bool> _cleaned;
    grpc::Status _status;
    bool _isRetry;
    opentelemetry::SpanPtr _span;
#ifdef SET_DEBUG
    ObjectPoolPtr _objectPool;
#endif
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcClientStreamHandler);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCCLIENTSTREAMHANDLER_H
