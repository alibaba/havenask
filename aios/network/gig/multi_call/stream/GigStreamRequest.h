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
#ifndef ISEARCH_MULTI_CALL_GIGSTREAMREQUEST_H
#define ISEARCH_MULTI_CALL_GIGSTREAMREQUEST_H

#include "aios/network/gig/multi_call/interface/Request.h"
#include "aios/network/gig/multi_call/stream/GigStreamMessage.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"
#include "aios/network/gig/multi_call/stream/GigStreamHandlerBase.h"
#include "autil/Lock.h"

namespace multi_call {

class GigClientStream;
class GrpcClientStreamHandler;
class CallBack;

struct SingleCallInfo {
    RequestType requestType;
    bool handlerInited : 1;
    bool sendEof : 1;
    std::shared_ptr<GrpcClientStreamHandler> handler;
    std::vector<GigStreamMessage> responseVec;
};

MULTI_CALL_TYPEDEF_PTR(SingleCallInfo);

class GigStreamRequest : public Request
{
public:
    GigStreamRequest(const std::shared_ptr<google::protobuf::Arena> &arena);
    ~GigStreamRequest();

private:
    GigStreamRequest(const GigStreamRequest &);
    GigStreamRequest &operator=(const GigStreamRequest &);

public:
    ResponsePtr newResponse() override;
    bool serialize() override { return true; }
    size_t size() const override { return 0; }

public:
    void setStream(GigClientStream *stream) {
        _stream.store(stream);
    }
    std::shared_ptr<GigStreamBase> getStream() const;
    bool beginRetryReceive() {
        return !_retryReceived.exchange(true);
    }
    void setDisableRetry(bool disable) {
        _disableRetry = disable;
    }
    void disableProbe(bool disable) {
        _disableProbe = disable;
    }
    void setForceStop(bool force) {
        _forceStop = force;
    }
    void addHandler(RequestType requestType,
                    const std::shared_ptr<GrpcClientStreamHandler> &handler,
                    bool isRetry);
    bool post(bool cancel, const GigStreamMessage &message);
    bool receive(SingleCallInfo *callInfo, const std::shared_ptr<GigStreamBase> &stream,
                 const GigStreamMessage &message);
    void receiveCancel(SingleCallInfo *callInfo, const std::shared_ptr<GigStreamBase> &stream,
                       const GigStreamMessage &message, MultiCallErrorCode ec);
    void abort();
    std::shared_ptr<GrpcClientStreamHandler> getResultHandler() const;
private:
    bool postNormal(const GigStreamMessage &message);
    bool flushRetryMessage();
    bool postSingleMessage(SingleCallInfo &callInfo, const SendBufferMessage &message);
    bool postCancel(const GigStreamMessage &message);
    void postSingleCancelMessage(const SingleCallInfo &callInfo,
                                 const SendBufferMessage &sendMessage,
                                 bool ignoreIfSendEof);
private:
    static bool receiveBufferedMessage(GigClientStream *stream, SingleCallInfo *callInfo);
private:
    bool _disableRetry : 1;
    bool _disableProbe : 1;
    bool _useRetryResult : 1;
    bool _forceStop : 1;
    std::atomic<GigClientStream *> _stream;
    SingleCallInfo *_normalCallInfo;
    std::vector<SingleCallInfo *> _otherCallInfos;
    std::atomic<bool> _retryReceived;
    autil::ThreadMutex _retrySendLock;
    SingleCallInfo *_retryCallInfo;
    std::vector<SendBufferMessage> _retryRequestVec;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigStreamRequest);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGSTREAMREQUEST_H
