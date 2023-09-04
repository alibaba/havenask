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
#include <functional>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/common/Common.h"
#include "swift/log/LogClosure.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/SwiftUuidGenerator.h"

namespace google {
namespace protobuf {
class Closure;
} // namespace protobuf
} // namespace google

namespace swift {
namespace service {
class BrokerPartition;

typedef std::shared_ptr<BrokerPartition> BrokerPartitionPtr;
} // namespace service
namespace util {

class ConsumptionLogClosure : public LogClosure<protocol::ConsumptionRequest, protocol::MessageResponse> {
public:
    ConsumptionLogClosure(const protocol::ConsumptionRequest *request,
                          protocol::MessageResponse *response,
                          google::protobuf::Closure *done,
                          const std::string &methodName)
        : LogClosure<protocol::ConsumptionRequest, protocol::MessageResponse>(request, response, done, methodName) {}
    virtual ~ConsumptionLogClosure() {
        uint64_t uuid = _request->requestuuid();
        _response->set_requestuuid(uuid);
        _response->mutable_tracetimeinfo()->set_requestgeneratedtime(_request->generatedtime());
        _response->mutable_tracetimeinfo()->set_requestrecivedtime(_queueTime);
        _response->mutable_tracetimeinfo()->set_responsedonetime(autil::TimeUtility::currentTime());
        IdData idData;
        idData.id = uuid;
        _forceLog = idData.requestId.logFlag;
        logAndDone();
        _autoDone = false;
    }

public:
    virtual void doAccessLog() {
        int64_t curTime = autil::TimeUtility::currentTime();
        int64_t usedTime = curTime - _beginTime;
        int64_t totalTime = curTime - _queueTime;
        const std::string &clientAddress = getClientAddress();
        const protocol::ErrorInfo &ecInfo = _response->errorinfo();
        const std::string &ecName = protocol::ErrorCode_Name(ecInfo.errcode());
        AUTIL_LOG(INFO,
                  "[%ldus %ldus]-[%s]-[%s]-[%s]-[%s]-["
                  "maxMsgId:%ld maxTimestamp:%ld nextMsgId:%ld nextTimestamp:%ld "
                  "sessionId:%ld messageFormat:%s "
                  "hasMergedMsg:%d totalMsgCount:%ld topicVersion:%ld traceInfo:%s longPollingInfo:%s]",
                  usedTime,
                  totalTime,
                  _methodName.c_str(),
                  ecName.c_str(),
                  clientAddress.c_str(),
                  _request->ShortDebugString().c_str(),
                  _response->maxmsgid(),
                  _response->maxtimestamp(),
                  _response->nextmsgid(),
                  _response->nexttimestamp(),
                  _response->sessionid(),
                  std::string(protocol::MessageFormat_Name(_response->messageformat())).c_str(),
                  _response->hasmergedmsg(),
                  _response->totalmsgcount(),
                  _response->topicversion(),
                  _response->tracetimeinfo().ShortDebugString().c_str(),
                  _response->longpollinginfo().ShortDebugString().c_str());
    }
};

class ProductionLogClosure : public LogClosure<protocol::ProductionRequest, protocol::MessageResponse> {
public:
    using BeforeLogCallBack = std::function<void(protocol::MessageResponse *)>;
    ProductionLogClosure(const protocol::ProductionRequest *request,
                         protocol::MessageResponse *response,
                         google::protobuf::Closure *done,
                         const std::string &methodName)
        : LogClosure<protocol::ProductionRequest, protocol::MessageResponse>(request, response, done, methodName) {}

    virtual ~ProductionLogClosure() {
        if (_beforeLogCb) {
            _beforeLogCb(_response);
        }
        _response->mutable_tracetimeinfo()->set_requestgeneratedtime(_request->generatedtime());
        _response->mutable_tracetimeinfo()->set_requestrecivedtime(_queueTime);
        _response->mutable_tracetimeinfo()->set_responsedonetime(autil::TimeUtility::currentTime());
        logAndDone();
        _autoDone = false;
    }

public:
    void setBeforeLogCallBack(BeforeLogCallBack cb) { _beforeLogCb = std::move(cb); }
    void setBrokerPartition(service::BrokerPartitionPtr brokerPartition) { _brokerPartition = brokerPartition; }
    virtual void doAccessLog() {
        int64_t curTime = autil::TimeUtility::currentTime();
        int64_t usedTime = curTime - _beginTime;
        int64_t totalTime = curTime - _queueTime;
        const std::string &clientAddress = getClientAddress();
        const protocol::ErrorInfo &ecInfo = _response->errorinfo();
        const std::string &ecName = protocol::ErrorCode_Name(ecInfo.errcode());
        AUTIL_LOG(INFO,
                  "[%ldus %ldus]-[%s]-[%s]-[%s]-[topicName:%s partitionId:%u requestId:%ld "
                  "compressMsgInBroker:%d sessionId:%ld messageFormat:%s pbmsgSize:%d "
                  "compressMsgLen:%zu fbmsgLen:%zu versionInfo{%s} id:%s]"
                  "-[acceptedMsgCount:%u maxAllowMsgLen:%u "
                  "acceptedMsgBeginId:%ld committedId:%ld sessionId:%ld messageFormat:%s "
                  "hasMergedMsg:%d totalMsgCount:%ld topicVersion:%ld, traceInfo:%s]",
                  usedTime,
                  totalTime,
                  _methodName.c_str(),
                  ecName.c_str(),
                  clientAddress.c_str(),
                  _request->topicname().c_str(),
                  _request->partitionid(),
                  _request->requestuuid(),
                  _request->compressmsginbroker(),
                  _request->sessionid(),
                  std::string(protocol::MessageFormat_Name(_request->messageformat())).c_str(),
                  _request->msgs_size(),
                  _request->compressedmsgs().size(),
                  _request->fbmsgs().size(),
                  _request->versioninfo().ShortDebugString().c_str(),
                  _request->identitystr().c_str(),
                  _response->acceptedmsgcount(),
                  _response->maxallowmsglen(),
                  _response->acceptedmsgbeginid(),
                  _response->committedid(),
                  _response->sessionid(),
                  std::string(protocol::MessageFormat_Name(_response->messageformat())).c_str(),
                  _response->hasmergedmsg(),
                  _response->totalmsgcount(),
                  _response->topicversion(),
                  _response->tracetimeinfo().ShortDebugString().c_str());
    }

private:
    BeforeLogCallBack _beforeLogCb;
    service::BrokerPartitionPtr _brokerPartition; // for wait all request handled when unload partition
};

class MessageIdLogClosure : public LogClosure<protocol::MessageIdRequest, protocol::MessageIdResponse> {
public:
    MessageIdLogClosure(const protocol::MessageIdRequest *request,
                        protocol::MessageIdResponse *response,
                        google::protobuf::Closure *done,
                        const std::string &methodName)
        : LogClosure<protocol::MessageIdRequest, protocol::MessageIdResponse>(request, response, done, methodName) {}
    virtual ~MessageIdLogClosure() {
        logAndDone();
        _autoDone = false;
    }

public:
    virtual void doAccessLog() {
        int64_t curTime = autil::TimeUtility::currentTime();
        int64_t usedTime = curTime - _beginTime;
        int64_t totalTime = curTime - _queueTime;
        const std::string &clientAddress = getClientAddress();
        const protocol::ErrorInfo &ecInfo = _response->errorinfo();
        const std::string &ecName = protocol::ErrorCode_Name(ecInfo.errcode());
        AUTIL_LOG(INFO,
                  "[%ldus %ldus]-[%s]-[%s]-[%s]-[%s]-[%s]",
                  usedTime,
                  totalTime,
                  _methodName.c_str(),
                  ecName.c_str(),
                  clientAddress.c_str(),
                  _request->ShortDebugString().c_str(),
                  _response->ShortDebugString().c_str());
    }
};

SWIFT_TYPEDEF_PTR(ConsumptionLogClosure);

} // namespace util
} // namespace swift
