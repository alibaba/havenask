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

#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "swift/client/SwiftTransportClient.h"
#include "swift/client/TransportClosure.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace protocol {
class ConsumptionRequest;
class MessageIdRequest;
class MessageIdResponse;
class MessageResponse;
class ProductionRequest;
} // namespace protocol

namespace client {

class FakeSwiftTransportClient : public SwiftTransportClient {
public:
    FakeSwiftTransportClient(const std::string &address = "",
                             network::SwiftRpcChannelManagerPtr channelManager = network::SwiftRpcChannelManagerPtr(),
                             const std::string &idStr = "");
    ~FakeSwiftTransportClient();

private:
    FakeSwiftTransportClient(const FakeSwiftTransportClient &);
    FakeSwiftTransportClient &operator=(const FakeSwiftTransportClient &);

public:
    bool getMessage(const ::swift::protocol::ConsumptionRequest *request,
                    ::swift::protocol::MessageResponse *response,
                    int64_t timeout = 30 * 1000) override;
    bool sendMessage(const ::swift::protocol::ProductionRequest *request,
                     ::swift::protocol::MessageResponse *response,
                     int64_t timeout = 30 * 1000) override;
    bool getMaxMessageId(const ::swift::protocol::MessageIdRequest *request,
                         ::swift::protocol::MessageIdResponse *response,
                         int64_t timeout = 30 * 1000) override;
    bool getMinMessageIdByTime(const ::swift::protocol::MessageIdRequest *request,
                               ::swift::protocol::MessageIdResponse *response,
                               int64_t timeout = 30 * 1000) override;

    void asyncCall(TransportClosureTyped<TRT_GETMESSAGE> *closure) override;
    void asyncCall(TransportClosureTyped<TRT_SENDMESSAGE> *closure) override;
    void asyncCall(TransportClosureTyped<TRT_GETMAXMESSAGEID> *closure) override;
    void asyncCall(TransportClosureTyped<TRT_GETMINMESSAGEIDBYTIME> *closure) override;

public:
    size_t getMsgCount() { return _messageVec->size(); }
    void createTopic(const std::string &topicName, uint32_t partCount);
    void clearMessage();
    void setTimeoutCount(int32_t timeoutCount) { _timeoutCount = timeoutCount; }
    void finishAsyncCall();
    void setAutoAsyncCall(bool autoCall = true) { _autoAsyncCall = autoCall; }
    void setCompressFlag(int compressFlag) { _compressFlag = compressFlag; }
    void setCommitMsgId(int64_t msgId) { _committedMsgId = msgId; }
    void setFakeDataMap(std::map<std::string, std::vector<swift::protocol::Message>> *fakeDataMap) {
        if (_fakeDataMap == NULL && _messageVec != NULL) {
            DELETE_AND_SET_NULL(_messageVec);
        }
        _fakeDataMap = fakeDataMap;
        _messageVec = &(*_fakeDataMap)[_address];
    }

public:
    void setErrorCode(swift::protocol::ErrorCode ec) { _ec = ec; }
    void clearMsg() { _messageVec->clear(); }
    void setUseRealTimeStamp(bool value = true) { _realTimeStamp = value; }
    void setRealRetTimeStamp(int64_t value) { _realRetTimeStamp = value; }

public:
    std::map<std::string, std::vector<swift::protocol::Message>> *_fakeDataMap;
    std::vector<swift::protocol::Message> *_messageVec;
    int32_t _timeoutCount;
    bool _autoAsyncCall;
    bool _finAsyncCall;
    // 0: no compress, 1: correctly request compress, 2: wrongly reqeust compress 4: correctly message compress 8: wrong
    // message compress
    int _compressFlag;

    // commit and acceptedMsgBeginId
    bool _supportClientSafeMode;
    int64_t _sessionId;
    int64_t _committedMsgId;
    int64_t _nextMsgId;
    bool _realTimeStamp;
    int64_t _realRetTimeStamp;
    swift::protocol::ErrorCode _ec;
    TransportClosureTyped<TRT_GETMESSAGE> *_getClosure;
    TransportClosureTyped<TRT_SENDMESSAGE> *_sendClosure;
    TransportClosureTyped<TRT_GETMAXMESSAGEID> *_getMaxMsgClosure;
    TransportClosureTyped<TRT_GETMINMESSAGEIDBYTIME> *_getMsgIdByTimeClosure;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeSwiftTransportClient);

} // namespace client
} // namespace swift
