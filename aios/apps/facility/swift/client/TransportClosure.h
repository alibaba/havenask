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
#pragma once

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>

#include "arpc/ANetRPCController.h"
#include "arpc/CommonMacros.h"
#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/client/Notifier.h"
#include "swift/common/Common.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/MessageCompressor.h"

namespace swift {
namespace client {

enum TransportRequestType {
    TRT_GETMESSAGE,
    TRT_SENDMESSAGE,
    TRT_GETMAXMESSAGEID,
    TRT_GETMINMESSAGEIDBYTIME,
};

template <TransportRequestType type>
struct TransportRequestTraits;

#define TRANSPORTREQUESTTRAITS_HELPER(enumType, requestType, responseType)                                             \
    template <>                                                                                                        \
    struct TransportRequestTraits<enumType> {                                                                          \
        typedef requestType RequestType;                                                                               \
        typedef responseType ResponseType;                                                                             \
    };

TRANSPORTREQUESTTRAITS_HELPER(TRT_GETMESSAGE, protocol::ConsumptionRequest, protocol::MessageResponse);
TRANSPORTREQUESTTRAITS_HELPER(TRT_SENDMESSAGE, protocol::ProductionRequest, protocol::MessageResponse);
TRANSPORTREQUESTTRAITS_HELPER(TRT_GETMAXMESSAGEID, protocol::MessageIdRequest, protocol::MessageIdResponse);
TRANSPORTREQUESTTRAITS_HELPER(TRT_GETMINMESSAGEIDBYTIME, protocol::MessageIdRequest, protocol::MessageIdResponse);
#undef TRANSPORTREQUESTTRAITS_HELPER

class TransportClosure : public RPCClosure, public std::enable_shared_from_this<TransportClosure> {
public:
    TransportClosure(TransportRequestType type,
                     Notifier *notifier,
                     int64_t timeout) // ms
        : _type(type), _notifier(notifier) {
        _beginTime = autil::TimeUtility::currentTime();
        _controller.SetExpireTime(timeout);
    }
    ~TransportClosure() {}

private:
    TransportClosure(const TransportClosure &);
    TransportClosure &operator=(const TransportClosure &);

public:
    void prepare() { _thisPtr = shared_from_this(); }
    void Run() {
        auto thisPtr = std::move(_thisPtr);
        _rpcDoneTime = autil::TimeUtility::currentTime();
        decompress();
        {
            autil::ScopedWriteLock lock(_rwLock);
            _doneTime = autil::TimeUtility::currentTime();
            _done = true;
            if (_notifier) {
                _notifier->notify();
                _notifier = nullptr;
            }
        }
    }
    void stop() {
        autil::ScopedWriteLock lock(_rwLock);
        if (_notifier) {
            _notifier->notify();
            _notifier = nullptr;
        }
    }
    arpc::ANetRPCController *getController() { return &_controller; }
    bool isArpcFailed() const { return _controller.Failed(); }
    std::string errorReason() const { return _controller.ErrorText(); }
    bool isDone() const {
        autil::ScopedReadLock lock(_rwLock);
        return _done;
    }
    TransportRequestType getType() const { return _type; }
    int64_t getDoneTime() const {
        autil::ScopedReadLock lock(_rwLock);
        return _doneTime;
    }
    int64_t getBeginTime() const { return _beginTime; }
    int64_t getRpcLatency() const { return _rpcDoneTime - _beginTime; }
    int64_t getNetworkLatency() const {
        if (_tracTimeInfo.has_responsedonetime()) {
            return _rpcDoneTime - _tracTimeInfo.responsedonetime();
        }
        return 0;
    }
    int64_t getDecompressLatency() const { return _doneTime - _rpcDoneTime; }
    virtual protocol::ErrorCode getResponseErrorCode() const = 0;
    virtual void decompress() {}
    virtual void log() {}
    void setRemoteAddress(const std::string &remoteAddress) { _remoteAddress = remoteAddress; }
    void setHandledTime(int64_t t) { _handledTime = t; }

protected:
    mutable autil::ReadWriteLock _rwLock;
    arpc::ANetRPCController _controller;
    const TransportRequestType _type;
    volatile bool _done = false;
    int64_t _beginTime = 0;
    int64_t _doneTime = -1;
    Notifier *_notifier = nullptr;
    int64_t _rpcDoneTime = -1;
    int64_t _handledTime = -1;
    std::string _remoteAddress;
    protocol::TraceTimeInfo _tracTimeInfo;
    std::shared_ptr<TransportClosure> _thisPtr;
};

SWIFT_TYPEDEF_PTR(TransportClosure);

template <TransportRequestType EnumType>
class TransportClosureTyped : public TransportClosure {
public:
    typedef typename TransportRequestTraits<EnumType>::RequestType RequestType;
    typedef typename TransportRequestTraits<EnumType>::ResponseType ResponseType;

public:
    TransportClosureTyped(const RequestType *request,
                          Notifier *notifier,
                          int64_t timeout = 30 * 1000) // 30s
        : TransportClosure(EnumType, notifier, timeout), _request(request) {
        _response = new ResponseType();
    }
    ~TransportClosureTyped();

public:
    const RequestType *getRequest() const { return _request; }
    ResponseType *stealResponse() {
        ResponseType *response = _response;
        _response = NULL;
        return response;
    }
    ResponseType *getResponse() { return _response; }
    protocol::ErrorCode getResponseErrorCode() const override { return _response->errorinfo().errcode(); }
    void decompress() override { _tracTimeInfo = _response->tracetimeinfo(); }
    void log() override;

private:
    const RequestType *_request;
    ResponseType *_response;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_WITH_TYPENAME(swift, TransportClosureTyped, TransportRequestType, EnumType);

template <TransportRequestType EnumType>
TransportClosureTyped<EnumType>::~TransportClosureTyped() {
    DELETE_AND_SET_NULL(_request);
    DELETE_AND_SET_NULL(_response);
}

template <TransportRequestType EnumType>
void TransportClosureTyped<EnumType>::log() {
    if (_request) {
        AUTIL_LOG(DEBUG,
                  "[%s %d] rpc time used[%ld] us, request id[%ld], request type[%d], "
                  "address[%s], done time[%ld], handled time[%ld], trace info[%s]",
                  _request->topicname().c_str(),
                  _request->partitionid(),
                  getRpcLatency(),
                  _request->requestuuid(),
                  EnumType,
                  _remoteAddress.c_str(),
                  _doneTime,
                  _handledTime,
                  _tracTimeInfo.ShortDebugString().c_str());
    }
}

template <>
class TransportClosureTyped<TRT_GETMESSAGE> : public TransportClosure {
public:
    typedef typename TransportRequestTraits<TRT_GETMESSAGE>::RequestType RequestType;
    typedef typename TransportRequestTraits<TRT_GETMESSAGE>::ResponseType ResponseType;

public:
    TransportClosureTyped(const RequestType *request,
                          Notifier *notifier,
                          int64_t timeout = 30 * 1000) // 30s
        : TransportClosure(TRT_GETMESSAGE, notifier, timeout), _request(request) {
        _response = new ResponseType();
    }
    ~TransportClosureTyped() {
        DELETE_AND_SET_NULL(_request);
        DELETE_AND_SET_NULL(_response);
    }

public:
    const RequestType *getRequest() const { return _request; }
    ResponseType *stealResponse() {
        ResponseType *response = _response;
        _response = NULL;
        return response;
    }
    ResponseType *getResponse() { return _response; }
    protocol::ErrorCode getResponseErrorCode() const override { return _response->errorinfo().errcode(); }
    void decompress() override {
        _tracTimeInfo = _response->tracetimeinfo();
        _readMsgCount = _response->totalmsgcount();
        _nextMsgId = _response->nextmsgid();
        _nextMsgTimestamp = _response->nexttimestamp();
        using namespace protocol;
        ErrorCode ec = getResponseErrorCode();
        if (ec != ERROR_NONE && ec != ERROR_BROKER_SOME_MESSAGE_LOST) {
            return;
        }
        if (_response->has_compressedmsgs()) {
            float ratio;
            ErrorCode ec2 = MessageCompressor::decompressMessageResponse(_response, ratio);
            if (ec2 != ERROR_NONE) {
                _response->mutable_errorinfo()->set_errcode(ERROR_CLIENT_INVALID_RESPONSE);
                return;
            }
        }
        if (_response->messageformat() == MF_FB) {
            FBMessageReader reader;
            if (!reader.init(_response->fbmsgs(), true)) {
                _response->mutable_errorinfo()->set_errcode(ERROR_CLIENT_INVALID_RESPONSE);
                return;
            }
        }
    }

    void log() override {
        if (_request) {
            AUTIL_LOG(DEBUG,
                      "[%s %d] rpc used[%ldus], request id[%ld], request type[GET_MESSAGE], "
                      "address[%s], read count[%ld], next msg id[%ld], next msg timestamp[%ld], done time[%ld], "
                      "handled time[%ld], trace info[%s]",
                      _request->topicname().c_str(),
                      _request->partitionid(),
                      getRpcLatency(),
                      _request->requestuuid(),
                      _remoteAddress.c_str(),
                      _readMsgCount,
                      _nextMsgId,
                      _nextMsgTimestamp,
                      _doneTime,
                      _handledTime,
                      _tracTimeInfo.ShortDebugString().c_str());
        }
    }

private:
    const RequestType *_request = nullptr;
    ResponseType *_response = nullptr;
    int64_t _readMsgCount = 0;
    int64_t _nextMsgId = 0;
    int64_t _nextMsgTimestamp = 0;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace client
} // namespace swift
