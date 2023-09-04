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
#include "aios/network/gig/multi_call/stream/GigStreamRequest.h"

#include "aios/network/gig/multi_call/grpc/client/GrpcClientStreamHandler.h"
#include "aios/network/gig/multi_call/stream/GigClientStream.h"
#include "aios/network/gig/multi_call/stream/GigStreamResponse.h"

using namespace std;
using namespace google::protobuf;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigStreamRequest);

GigStreamRequest::GigStreamRequest(const std::shared_ptr<Arena> &arena)
    : Request(MC_PROTOCOL_GRPC_STREAM, arena)
    , _disableRetry(false)
    , _disableProbe(false)
    , _useRetryResult(false)
    , _forceStop(false)
    , _stream(nullptr)
    , _normalCallInfo(nullptr)
    , _retryReceived(false)
    , _retryCallInfo(nullptr) {
}

GigStreamRequest::~GigStreamRequest() {
    if (_normalCallInfo) {
        DELETE_AND_SET_NULL(_normalCallInfo);
    }
    if (_retryCallInfo) {
        DELETE_AND_SET_NULL(_retryCallInfo);
    }
    for (auto callInfo : _otherCallInfos) {
        DELETE_AND_SET_NULL(callInfo);
    }
}

ResponsePtr GigStreamRequest::newResponse() {
    return ResponsePtr(new GigStreamResponse());
}

std::shared_ptr<GigStreamBase> GigStreamRequest::getStream() const {
    if (_normalCallInfo) {
        return _normalCallInfo->handler->getStream();
    } else {
        auto stream = _stream.load();
        return stream->shared_from_this();
    }
}

void GigStreamRequest::addHandler(RequestType requestType,
                                  const std::shared_ptr<GrpcClientStreamHandler> &handler,
                                  bool isRetry) {
    auto callInfo = new SingleCallInfo();
    callInfo->requestType = requestType;
    callInfo->handlerInited = false;
    callInfo->sendEof = false;
    callInfo->handler = handler;
    handler->setCallInfo(callInfo);
    if (isRetry) {
        _retryCallInfo = callInfo;
        MULTI_CALL_MEMORY_BARRIER();
        flushRetryMessage();
        return;
    }
    if (RT_NORMAL == requestType) {
        _normalCallInfo = callInfo;
    } else {
        _otherCallInfos.emplace_back(callInfo);
    }
}

bool GigStreamRequest::post(bool cancel, const GigStreamMessage &message) {
    if (!cancel) {
        return postNormal(message);
    } else {
        return postCancel(message);
    }
}

bool GigStreamRequest::postNormal(const GigStreamMessage &message) {
    auto eof = message.eof;
    bool normalOk = false;
    auto sendMessage = GigStreamHandlerBase::createSendBufferMessage(eof, false, message.message);
    if (_normalCallInfo && postSingleMessage(*_normalCallInfo, sendMessage)) {
        normalOk = true;
    }
    if (!_disableRetry) {
        {
            autil::ScopedLock lock(_retrySendLock);
            _retryRequestVec.push_back(sendMessage);
        }
        if (flushRetryMessage()) {
            normalOk = true;
        }
    }
    for (const auto &ptr : _otherCallInfos) {
        postSingleMessage(*ptr, sendMessage);
    }
    if (_normalCallInfo) {
        return normalOk;
    } else {
        return !eof;
    }
}

bool GigStreamRequest::flushRetryMessage() {
    if (!_retryCallInfo) {
        return false;
    }
    std::vector<SendBufferMessage> clearRequestVec;
    if (0 != _retrySendLock.trylock()) {
        return true;
    }
    if (_retryRequestVec.empty()) {
        _retrySendLock.unlock();
        return true;
    }
    auto callInfo = _retryCallInfo;
    clearRequestVec.swap(_retryRequestVec);
    for (const auto &message : clearRequestVec) {
        if (!message.cancel) {
            if (!postSingleMessage(*callInfo, message)) {
                _retrySendLock.unlock();
                return false;
            }
        } else {
            postSingleCancelMessage(*callInfo, message, true);
        }
    }
    _retrySendLock.unlock();
    return true;
}

bool GigStreamRequest::postSingleMessage(SingleCallInfo &callInfo,
                                         const SendBufferMessage &message) {
    const auto &handler = callInfo.handler;
    if (!handler) {
        return false;
    }
    if (!callInfo.handlerInited) {
        if (!handler->init()) {
            return false;
        } else {
            callInfo.handlerInited = true;
        }
    }
    AUTIL_LOG(DEBUG, "handler [%p], send message eof [%d]", handler.get(), message.eof);
    if (!handler->send(message)) {
        return false;
    } else {
        callInfo.sendEof = message.eof;
        return true;
    }
}

bool GigStreamRequest::postCancel(const GigStreamMessage &message) {
    auto sendMessage = GigStreamHandlerBase::createSendBufferMessage(true, true, message.message);
    sendMessage.forceStop = _forceStop;
    auto ignoreIfSendEof = _disableProbe ? false : true;
    if (_normalCallInfo) {
        postSingleCancelMessage(*_normalCallInfo, sendMessage, ignoreIfSendEof);
    }
    if (!_disableRetry) {
        {
            autil::ScopedLock lock(_retrySendLock);
            _retryRequestVec.push_back(sendMessage);
        }
        flushRetryMessage();
    }
    for (const auto &ptr : _otherCallInfos) {
        postSingleCancelMessage(*ptr, sendMessage, true);
    }
    return true;
}

void GigStreamRequest::postSingleCancelMessage(const SingleCallInfo &callInfo,
                                               const SendBufferMessage &sendMessage,
                                               bool ignoreIfSendEof) {
    const auto &handler = callInfo.handler;
    if (!handler) {
        return;
    }
    bool ignore = ignoreIfSendEof && callInfo.sendEof;
    if (!callInfo.handlerInited) {
        handler->abort();
    } else if (!ignore) {
        AUTIL_LOG(DEBUG, "handler [%p] requestType [%d], send cancel", handler.get(),
                  callInfo.requestType);
        handler->send(sendMessage);
    }
}

bool GigStreamRequest::receive(SingleCallInfo *callInfo,
                               const std::shared_ptr<GigStreamBase> &stream,
                               const GigStreamMessage &message) {
    if (RT_NORMAL != callInfo->requestType) {
        return true;
    }
    auto clientStream = static_cast<GigClientStream *>(stream.get());
    if (_disableRetry) {
        if (clientStream) {
            return clientStream->innerReceive(message, MULTI_CALL_ERROR_NONE);
        } else {
            return false;
        }
    }
    callInfo->responseVec.push_back(message);
    if (!message.eof) {
        return true;
    }
    if (!beginRetryReceive()) {
        return true;
    }
    if (callInfo == _retryCallInfo) {
        _useRetryResult = true;
    }
    return receiveBufferedMessage(clientStream, callInfo);
}

void GigStreamRequest::receiveCancel(SingleCallInfo *callInfo,
                                     const std::shared_ptr<GigStreamBase> &stream,
                                     const GigStreamMessage &message, MultiCallErrorCode ec) {
    if (RT_NORMAL != callInfo->requestType) {
        return;
    }
    auto clientStream = static_cast<GigClientStream *>(stream.get());
    if (_disableRetry) {
        if (clientStream) {
            clientStream->innerReceive(message, ec);
        }
        return;
    }
    if (!beginRetryReceive()) {
        return;
    }
    if (callInfo == _retryCallInfo) {
        _useRetryResult = true;
    }
    if (!receiveBufferedMessage(clientStream, callInfo)) {
        return;
    }
    clientStream->innerReceive(message, ec);
}

bool GigStreamRequest::receiveBufferedMessage(GigClientStream *stream, SingleCallInfo *callInfo) {
    auto &responseVec = callInfo->responseVec;
    for (const auto &oldMessage : responseVec) {
        if (!stream->innerReceive(oldMessage, MULTI_CALL_ERROR_NONE)) {
            return false;
        }
    }
    responseVec.clear();
    return true;
}

void GigStreamRequest::abort() {
    if (_normalCallInfo) {
        _normalCallInfo->handler->abort();
    }
    if (_retryCallInfo) {
        _retryCallInfo->handler->abort();
    }
    for (const auto &callInfo : _otherCallInfos) {
        callInfo->handler->abort();
    }
}

std::shared_ptr<GrpcClientStreamHandler> GigStreamRequest::getResultHandler() const {
    if (!_useRetryResult && _normalCallInfo) {
        return _normalCallInfo->handler;
    } else if (_retryCallInfo) {
        return _retryCallInfo->handler;
    } else {
        return nullptr;
    }
}

} // namespace multi_call
