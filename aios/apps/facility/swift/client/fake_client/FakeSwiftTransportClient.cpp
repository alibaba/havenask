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
#include "swift/client/fake_client/FakeSwiftTransportClient.h"

#include <algorithm>
#include <cstddef>
#include <google/protobuf/stubs/port.h>
#include <memory>

#include "autil/TimeUtility.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/MessageCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace swift;
using namespace google::protobuf;
using namespace autil;

namespace swift {
namespace client {
using namespace swift::protocol;
using namespace swift::network;
AUTIL_LOG_SETUP(swift, FakeSwiftTransportClient);

FakeSwiftTransportClient::FakeSwiftTransportClient(const std::string &address,
                                                   SwiftRpcChannelManagerPtr channelManager,
                                                   const std::string &idStr)
    : SwiftTransportClient(address, channelManager, idStr)
    , _timeoutCount(0)
    , _autoAsyncCall(false)
    , _finAsyncCall(true)
    , _compressFlag(0)
    , _supportClientSafeMode(false)
    , _sessionId(TimeUtility::currentTimeInMicroSeconds())
    , _committedMsgId(-1)
    , _nextMsgId(-1)
    , _realTimeStamp(false)
    , _realRetTimeStamp(0)
    , _ec(ERROR_NONE)
    , _getClosure(NULL)
    , _sendClosure(NULL)
    , _getMaxMsgClosure(NULL)
    , _getMsgIdByTimeClosure(NULL) {
    _fakeDataMap = NULL;
    _messageVec = new std::vector<swift::protocol::Message>();
}

FakeSwiftTransportClient::~FakeSwiftTransportClient() {
    if (_finAsyncCall) {
        finishAsyncCall();
    }
    if (_fakeDataMap == NULL && _messageVec != NULL) {
        clearMessage();
        DELETE_AND_SET_NULL(_messageVec);
    }
}

bool FakeSwiftTransportClient::getMessage(const ConsumptionRequest *request,
                                          MessageResponse *response,
                                          int64_t timeout) {
    (void)timeout;
    response->Clear();
    if (_timeoutCount > 0) {
        --_timeoutCount;
        return false;
    }
    if (ERROR_BROKER_SESSION_CHANGED == _ec) {
        _sessionId = TimeUtility::currentTimeInMicroSeconds();
    }
    response->set_sessionid(_sessionId);
    ErrorInfo *errorInfo = response->mutable_errorinfo();
    if (ERROR_NONE != _ec) {
        if (ERROR_CLIENT_INVALID_RESPONSE == _ec) {
            // max msg id not set
        } else {
            if (ERROR_BROKER_SESSION_CHANGED == _ec) {
                //               _ec = ERROR_NONE;
            }
            errorInfo->set_errcode(_ec);
            errorInfo->set_errmsg("");
        }
        return true;
    }

    int64_t startId = request->startid();
    int64_t count = request->count();
    bool foundStartId = false;
    vector<swift::protocol::Message>::const_iterator it;
    for (it = _messageVec->begin(); it != _messageVec->end(); ++it) {
        if ((*it).msgid() >= startId) {
            foundStartId = true;
            break;
        }
    }

    if (!foundStartId) {
        // broker[:], get from start=1, count=3
        if (_messageVec->size() == 0) {
            errorInfo->set_errcode(ERROR_BROKER_NO_DATA);
            errorInfo->set_errmsg("broker no data!");
            response->set_nextmsgid(0);
            if (_realTimeStamp) {
                response->set_nexttimestamp(_realRetTimeStamp);
            } else {
                response->set_nexttimestamp(autil::TimeUtility::currentTime());
            }
        } else {
            response->set_maxmsgid((*_messageVec)[_messageVec->size() - 1].msgid());
            response->set_maxtimestamp((*_messageVec)[_messageVec->size() - 1].timestamp());
            response->set_nextmsgid(min(startId + count, ((*_messageVec)[_messageVec->size() - 1].msgid() + 1)));
            auto retTime = (*_messageVec)[_messageVec->size() - 1].timestamp() + 1;
            if (_realTimeStamp) {
                retTime = _realRetTimeStamp; // max(retTime, TimeUtility::currentTime());
            }
            response->set_nexttimestamp(retTime);
            errorInfo->set_errcode(ERROR_NONE);
            errorInfo->set_errmsg("");
        }
        return true;
    }
    response->set_maxmsgid((*_messageVec)[_messageVec->size() - 1].msgid());
    response->set_maxtimestamp((*_messageVec)[_messageVec->size() - 1].timestamp());
    for (; it != _messageVec->end() && it->msgid() < startId + count; ++it) {
        swift::protocol::Message *message = response->add_msgs();
        message->set_msgid(it->msgid());
        message->set_timestamp(it->timestamp());
        message->set_data(it->data());
        message->set_compress(it->compress());
        message->set_merged(it->merged());
    }

    if ((it != _messageVec->end() && response->msgs_size() < count) || startId < (*_messageVec)[0].msgid()) {
        errorInfo->set_errcode(ERROR_BROKER_SOME_MESSAGE_LOST);
        errorInfo->set_errmsg("ERROR_BROKER_SOME_MESSAGE_LOST");
    }
    response->set_nextmsgid(min(startId + count, ((*_messageVec)[_messageVec->size() - 1].msgid() + 1)));
    if (it == _messageVec->end()) {
        auto retTime = (*_messageVec)[_messageVec->size() - 1].timestamp() + 1;
        if (_realTimeStamp) {
            retTime = _realRetTimeStamp; // max(retTime, TimeUtility::currentTime());
        }
        response->set_nexttimestamp(retTime);
    } else {
        response->set_nexttimestamp(it->timestamp());
    }
    if ((_compressFlag & 0x08) && response->msgs_size() > 0) {
        for (int i = 0; i < response->msgs_size(); i++) {
            Message *msg = response->mutable_msgs(i);
            msg->set_data("wrong message data");
            msg->set_compress(true);
        }
    }
    if ((_compressFlag & 0x04) && response->msgs_size() > 0) {
        MessageCompressor::compressResponseMessage(response);
    }

    if ((_compressFlag & 0x01) && response->msgs_size() > 0) {
        MessageCompressor::compressMessageResponse(response);
    }
    if (_compressFlag & 0x02) {
        protocol::Messages tmpMessage;
        tmpMessage.mutable_msgs()->Swap(response->mutable_msgs());
        response->set_compressedmsgs("wrongly compressed response");
    }
    return true;
}

bool FakeSwiftTransportClient::sendMessage(const ProductionRequest *request,
                                           MessageResponse *response,
                                           int64_t timeout) {
    (void)timeout;
    if (_timeoutCount > 0) {
        --_timeoutCount;
        return false;
    }
    if (ERROR_BROKER_SESSION_CHANGED == _ec) {
        _sessionId = TimeUtility::currentTimeInMicroSeconds();
    }
    response->set_sessionid(_sessionId);
    ErrorInfo *errorInfo = response->mutable_errorinfo();

    if (ERROR_BROKER_STOPPED == _ec || ERROR_BROKER_SESSION_CHANGED == _ec || ERROR_BROKER_SESSION_CHANGED == _ec) {
        if (ERROR_BROKER_SESSION_CHANGED == _ec) {
            response->set_acceptedmsgcount(0);
        }
        errorInfo->set_errcode(_ec);
        errorInfo->set_errmsg("");
        return true;
    }

    if (request->has_compressedmsgs()) {
        float ratio;
        ErrorCode ec = MessageCompressor::decompressProductionRequest(const_cast<ProductionRequest *>(request), ratio);
        if (ec != ERROR_NONE) {
            return false;
        }
    }
    int64_t acceptedMsgBeginId = _nextMsgId;
    for (int i = 0; i < request->msgs_size(); ++i) {
        Message msg(request->msgs(i));
        if (_nextMsgId != -1) {
            msg.set_msgid(_nextMsgId);
            msg.set_timestamp(_nextMsgId * 10);
            response->add_timestamps(_nextMsgId * 10);
            ++_nextMsgId;
        }
        _messageVec->push_back(msg);
    }
    response->set_acceptedmsgcount(request->msgs_size());
    if (_supportClientSafeMode) {
        response->set_sessionid(_sessionId);
        response->set_committedid(_committedMsgId);
        response->set_acceptedmsgbeginid(acceptedMsgBeginId);
    }
    if (request->has_sessionid() && _supportClientSafeMode && request->sessionid() != -1 &&
        request->sessionid() != _sessionId) {
        errorInfo->set_errcode(ERROR_BROKER_SESSION_CHANGED);
    } else {
        errorInfo->set_errcode(ERROR_NONE);
    }
    errorInfo->set_errmsg("");
    return true;
}

bool FakeSwiftTransportClient::getMaxMessageId(const MessageIdRequest *request,
                                               MessageIdResponse *response,
                                               int64_t timeout) {
    (void)request;
    (void)timeout;
    if (_timeoutCount > 0) {
        --_timeoutCount;
        return false;
    }
    response->set_sessionid(_sessionId);

    ErrorInfo *errorInfo = response->mutable_errorinfo();
    if (ERROR_NONE != _ec) {
        errorInfo->set_errcode(_ec);
        errorInfo->set_errmsg("");
        return true;
    }

    if (_messageVec->empty()) {
        errorInfo->set_errcode(ERROR_BROKER_NO_DATA);
        errorInfo->set_errmsg("");
        return true;
    }
    swift::protocol::Message &message = *_messageVec->rbegin();
    response->set_msgid(message.msgid());
    response->set_timestamp(message.timestamp());
    errorInfo->set_errcode(ERROR_NONE);
    errorInfo->set_errmsg("");
    return true;
}

bool FakeSwiftTransportClient::getMinMessageIdByTime(const MessageIdRequest *request,
                                                     MessageIdResponse *response,
                                                     int64_t timeout) {
    (void)timeout;
    if (_timeoutCount > 0) {
        --_timeoutCount;
        return false;
    }

    response->set_sessionid(_sessionId);
    ErrorInfo *errorInfo = response->mutable_errorinfo();
    if (ERROR_NONE != _ec) {
        if (ERROR_CLIENT_INVALID_RESPONSE == _ec) {
            // max msg id not set
        } else {
            errorInfo->set_errcode(_ec);
            errorInfo->set_errmsg("");
        }
        return true;
    }

    int64_t timestamp = request->timestamp();
    vector<swift::protocol::Message>::const_iterator it = _messageVec->begin();
    for (; it != _messageVec->end(); ++it) {
        if (it->timestamp() >= timestamp) {
            break;
        }
    }

    if (it == _messageVec->end()) {
        if (0 == _messageVec->size()) {
            errorInfo->set_errcode(ERROR_BROKER_NO_DATA);
            errorInfo->set_errmsg("");
            return true;
        }
        response->set_maxmsgid((*_messageVec)[_messageVec->size() - 1].msgid());
        response->set_maxtimestamp((*_messageVec)[_messageVec->size() - 1].timestamp());
        errorInfo->set_errcode(ERROR_BROKER_TIMESTAMP_TOO_LATEST);
        errorInfo->set_errmsg("");
        return true;
    }

    response->set_maxmsgid((*_messageVec)[_messageVec->size() - 1].msgid());
    response->set_maxtimestamp((*_messageVec)[_messageVec->size() - 1].timestamp());
    response->set_msgid(it->msgid());
    response->set_timestamp(it->timestamp());

    errorInfo->set_errcode(ERROR_NONE);
    errorInfo->set_errmsg("");
    return true;
}

void FakeSwiftTransportClient::clearMessage() {
    if (_messageVec) {
        _messageVec->clear();
    }
}

void FakeSwiftTransportClient::finishAsyncCall() {
    if (_getClosure) {
        getMessage(_getClosure->getRequest(), _getClosure->getResponse());
        _getClosure->Run();
        _getClosure = NULL;
    }

    if (_sendClosure) {
        sendMessage(_sendClosure->getRequest(), _sendClosure->getResponse());
        _sendClosure->Run();
        _sendClosure = NULL;
    }

    if (_getMaxMsgClosure) {
        getMaxMessageId(_getMaxMsgClosure->getRequest(), _getMaxMsgClosure->getResponse());
        _getMaxMsgClosure->Run();
        _getMaxMsgClosure = NULL;
    }

    if (_getMsgIdByTimeClosure) {
        getMinMessageIdByTime(_getMsgIdByTimeClosure->getRequest(), _getMsgIdByTimeClosure->getResponse());
        _getMsgIdByTimeClosure->Run();
        _getMsgIdByTimeClosure = NULL;
    }
}

void FakeSwiftTransportClient::asyncCall(TransportClosureTyped<TRT_GETMESSAGE> *closure) {
    _getClosure = closure;
    if (_autoAsyncCall) {
        finishAsyncCall();
    }
}

void FakeSwiftTransportClient::asyncCall(TransportClosureTyped<TRT_SENDMESSAGE> *closure) {
    _sendClosure = closure;
    if (_autoAsyncCall) {
        finishAsyncCall();
    }
}

void FakeSwiftTransportClient::asyncCall(TransportClosureTyped<TRT_GETMAXMESSAGEID> *closure) {
    _getMaxMsgClosure = closure;
    if (_autoAsyncCall) {
        finishAsyncCall();
    }
}

void FakeSwiftTransportClient::asyncCall(TransportClosureTyped<TRT_GETMINMESSAGEIDBYTIME> *closure) {
    _getMsgIdByTimeClosure = closure;
    if (_autoAsyncCall) {
        finishAsyncCall();
    }
}

} // namespace client
} // namespace swift
