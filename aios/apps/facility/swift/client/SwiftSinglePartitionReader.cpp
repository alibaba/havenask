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
#include "swift/client/SwiftSinglePartitionReader.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <unistd.h>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/trace/SwiftErrorResponseCollector.h"
#include "swift/client/trace/SwiftFatalErrorCollector.h"
#include "swift/config/ClientAuthorizerInfo.h"
#include "swift/monitor/ClientMetricsReporter.h"
#include "swift/monitor/MetricsCommon.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/protocol/TraceMessage.pb.h"
#include "swift/util/Atomic.h"
#include "swift/util/IpUtil.h"
#include "swift/util/SwiftUuidGenerator.h"
#include "swift/version.h"

namespace swift {
namespace client {
class Notifier;
} // namespace client
} // namespace swift

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::network;
using namespace swift::util;
using namespace swift::monitor;

namespace swift {
namespace client {

AUTIL_LOG_SETUP(swift, SwiftSinglePartitionReader);

static const int64_t MAX_TIME_STAMP = numeric_limits<int64_t>::max();

#define HANDLE_SESSION_OR_PARTITION_CHANGE                                                                             \
    if (_lastTopicVersion == -1 && response->has_topicversion()) {                                                     \
        _lastTopicVersion = response->topicversion();                                                                  \
    }                                                                                                                  \
    if (ec == ERROR_BROKER_SESSION_CHANGED || ec == ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND) {                          \
        if (_func) {                                                                                                   \
            int64_t topicVersion = 0;                                                                                  \
            if (response->has_topicversion()) {                                                                        \
                topicVersion = response->topicversion();                                                               \
            }                                                                                                          \
            ErrorCode funcEc;                                                                                          \
            if (_lastTopicVersion > 0 || topicVersion > 0) {                                                           \
                if (topicVersion != _lastTopicVersion) {                                                               \
                    if (topicVersion != 0) {                                                                           \
                        funcEc = _func(topicVersion);                                                                  \
                    } else {                                                                                           \
                        funcEc = _func(_lastTopicVersion);                                                             \
                    }                                                                                                  \
                    if (funcEc != ERROR_NONE) {                                                                        \
                        return funcEc;                                                                                 \
                    }                                                                                                  \
                    if (topicVersion != 0) {                                                                           \
                        AUTIL_LOG(INFO,                                                                                \
                                  "[%s %d] topic version changed[%ld -> %ld]",                                         \
                                  _config.topicName.c_str(),                                                           \
                                  _partitionId,                                                                        \
                                  _lastTopicVersion,                                                                   \
                                  topicVersion);                                                                       \
                        _lastTopicVersion = topicVersion;                                                              \
                    }                                                                                                  \
                }                                                                                                      \
            } else {                                                                                                   \
                AUTIL_LOG(INFO,                                                                                        \
                          "[%s %d] topic version may changed, error[%s]",                                              \
                          _config.topicName.c_str(),                                                                   \
                          _partitionId,                                                                                \
                          ErrorCode_Name(ec).c_str());                                                                 \
                funcEc = _func(topicVersion);                                                                          \
                if (funcEc != ERROR_NONE) {                                                                            \
                    return funcEc;                                                                                     \
                }                                                                                                      \
            }                                                                                                          \
        }                                                                                                              \
        if (response->has_sessionid()) {                                                                               \
            AUTIL_LOG(INFO,                                                                                            \
                      "[%s %d] sessionid changed[%ld -> %ld]",                                                         \
                      _config.topicName.c_str(),                                                                       \
                      _partitionId,                                                                                    \
                      _sessionId,                                                                                      \
                      response->sessionid());                                                                          \
            _sessionId = response->sessionid();                                                                        \
        }                                                                                                              \
        return ec;                                                                                                     \
    } else {                                                                                                           \
        if (response->has_topicversion()) {                                                                            \
            int64_t topicVersion = response->topicversion();                                                           \
            if (topicVersion > 0 && _lastTopicVersion != topicVersion) {                                               \
                ErrorCode funcEc = _func(topicVersion);                                                                \
                AUTIL_LOG(INFO,                                                                                        \
                          "[%s %d] topic version changed[%ld -> %ld], ret[%s]",                                        \
                          _config.topicName.c_str(),                                                                   \
                          _partitionId,                                                                                \
                          _lastTopicVersion,                                                                           \
                          topicVersion,                                                                                \
                          ErrorCode_Name(funcEc).c_str());                                                             \
                if (funcEc != ERROR_NONE) {                                                                            \
                    return funcEc;                                                                                     \
                }                                                                                                      \
                _lastTopicVersion = topicVersion;                                                                      \
            }                                                                                                          \
        }                                                                                                              \
    }

SwiftSinglePartitionReader::SwiftSinglePartitionReader(SwiftAdminAdapterPtr adminAdapter,
                                                       SwiftRpcChannelManagerPtr channelManager,
                                                       uint32_t partitionId,
                                                       const SwiftReaderConfig &config,
                                                       int64_t topicVersion,
                                                       Notifier *notifier,
                                                       monitor::ClientMetricsReporter *reporter)
    : _adminAdapter(adminAdapter)
    , _partitionId(partitionId)
    , _config(config)
    , _timestampLimit(MAX_TIME_STAMP)
    , _lastMsgTimestamp(-1)
    , _lastMsgOffsetInMerge(0)
    , _lastCheckpointTimestamp(-1)
    , _func(NULL)
    , _sessionId(-1)
    , _clientCommittedCheckpoint(-1)
    , _lastTopicVersion(topicVersion)
    , _sealedTopicReadFinish(false)
    , _isTopicLongPollingEnabled(false)
    , _metricsReporter(reporter) {
    _transportAdapter = new SwiftTransportAdapter<TRT_GETMESSAGE>(
        adminAdapter, channelManager, _config.topicName, partitionId, notifier, _config.retryGetMsgInterval);
    string idStr = "read " + _config.topicName + " " + autil::StringUtil::toString(_partitionId);
    _transportAdapter->setIdStr(idStr);
    protocol::AuthenticationInfo authInfo;
    *authInfo.mutable_accessauthinfo() = config.getAccessAuthInfo();
    if (_adminAdapter) {
        auto clientAuthInfo = _adminAdapter->getAuthenticationConf();
        if (!clientAuthInfo.isEmpty()) {
            authInfo.set_username(clientAuthInfo.username);
            authInfo.set_passwd(clientAuthInfo.passwd);
        }
    }
    _transportAdapter->setAuthInfo(authInfo);

    _maxMsgIdTransportAdapter = new SwiftTransportAdapter<TRT_GETMAXMESSAGEID>(
        adminAdapter, channelManager, _config.topicName, partitionId, NULL);
    idStr = "read_maxid " + _config.topicName + " " + autil::StringUtil::toString(_partitionId);
    _maxMsgIdTransportAdapter->setIdStr(idStr);
    _maxMsgIdTransportAdapter->setAuthInfo(authInfo);

    _msgIdTransportAdapter = new SwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME>(
        adminAdapter, channelManager, _config.topicName, _partitionId);
    idStr = "read_id " + _config.topicName + " " + autil::StringUtil::toString(_partitionId);
    _msgIdTransportAdapter->setIdStr(idStr);
    _msgIdTransportAdapter->setAuthInfo(authInfo);

    _clientVersionInfo.set_version(swift_client_version_str);
    if (_config.messageFormat == 1) {
        _clientVersionInfo.set_supportfb(true);
    } else {
        _clientVersionInfo.set_supportfb(false);
    }
    if (_config.mergeMessage == 1) {
        _clientVersionInfo.set_supportmergemsg(true);
    } else {
        _clientVersionInfo.set_supportmergemsg(false);
    }
    _clientVersionInfo.set_clienttype(CT_CPP);
    _clientVersionInfo.set_supportauthentication(true);
    clear();
    _buffer.updateFilter(_config.swiftFilter, _requiredFieldNames, _filedFilterDesc);
    _buffer.setTopicName(_config.topicName);
    _buffer.setPartitionId(_partitionId);
    _readerInfo.set_topicname(_config.topicName);
    _readerInfo.set_partitionid(_partitionId);
    _readerInfo.set_readerid(_config.readerIdentity);
    _readerInfo.set_pid(getpid());
    _readerInfo.set_ip(IpUtil::getIp());
    *_readerInfo.mutable_filter() = _config.swiftFilter;
    _buffer.setReaderInfo(_readerInfo);
    _metricsTags.AddTag("topic", config.topicName);
    _metricsTags.AddTag("partition", intToString(_partitionId));
    if (_config.checkpointMode.empty() || _config.checkpointMode == READER_CHECKPOINT_REFRESH_MODE) {
        _checkpointReflushMode = true;
    } else {
        _checkpointReflushMode = false;
    }
}

SwiftSinglePartitionReader::~SwiftSinglePartitionReader() {
    DELETE_AND_SET_NULL(_transportAdapter);
    DELETE_AND_SET_NULL(_maxMsgIdTransportAdapter);
    DELETE_AND_SET_NULL(_msgIdTransportAdapter);
}

void SwiftSinglePartitionReader::clear() {
    _partitionStatus.refreshTime = -1;
    _partitionStatus.maxMessageId = -1;
    _partitionStatus.maxMessageTimestamp = -1;
    _buffer.clear();
    _nextMsgId = 0;
    _nextTimestamp = -1;
    _seekTimestamp = -1;
    _lastErrorCode = ERROR_NONE;
    _errorCodeForCheck = ERROR_NONE;
    _lastSuccessResponseTime = TimeUtility::currentTime();
    _lastReportErrorTime = TimeUtility::currentTime();
    _seekByTimestampRecord.lastErrorCode = ERROR_NONE;
    _seekByTimestampRecord.lastSeekTime = TimeUtility::currentTime();
    _lastRefreshMsgIdTime = 0;
    _timestampLimit = MAX_TIME_STAMP;
    _lastMsgTimestamp = -1;
    _lastMsgOffsetInMerge = 0;
    _lastCheckpointTimestamp = -1;
    _func = NULL;
    _sessionId = -1;
}

bool SwiftSinglePartitionReader::exceedTimestampLimit() {
    if (_timestampLimit == MAX_TIME_STAMP) {
        return false;
    }
    int64_t timestamp = getNextMsgTimestamp();
    if (timestamp == -1) {
        return false;
    }
    return timestamp > _timestampLimit;
}

bool SwiftSinglePartitionReader::tryRead(protocol::Messages &msgs) {
    msgs.clear_msgs();
    uint32_t maxReadCount = _config.batchReadCount;
    Message tmpMessage;
    while (maxReadCount-- > 0) {
        if (_buffer.getFirstMsgTimestamp() > _timestampLimit) {
            break;
        }
        if (_buffer.read(tmpMessage)) {
            if (tmpMessage.timestamp() < _lastMsgTimestamp) {
                AUTIL_LOG(WARN,
                          "[%s %d] message timestamp roll back, msg id [%ld], timestamp [%ld], last msg timestamp[%ld]",
                          _config.topicName.c_str(),
                          _partitionId,
                          tmpMessage.msgid(),
                          tmpMessage.timestamp(),
                          _lastMsgTimestamp);
            }
            _lastMsgTimestamp = tmpMessage.timestamp();
            _lastMsgOffsetInMerge = tmpMessage.offsetinrawmsg();
            msgs.add_msgs()->Swap(&tmpMessage);
        } else if (getUnReadMsgCount() <= 0) {
            break;
        }
    }
    return msgs.msgs_size() > 0;
}

int64_t SwiftSinglePartitionReader::tryFillBuffer(int64_t currentTime) {
    bool isSend = false;
    return tryFillBuffer(currentTime, false, isSend);
}

int64_t SwiftSinglePartitionReader::tryFillBuffer(int64_t currentTime, bool force, bool &isSent) {
    ScopedLock lock(_mutex);
    int64_t retryInterval = MAX_TIME_STAMP;
    isSent = false;
    if (!_transportAdapter->isLastRequestDone()) {
        return MAX_TIME_STAMP;
    }

    ErrorCode ec = ERROR_NONE;
    ClientMetricsCollector collector;
    if (!_transportAdapter->isLastRequestHandled()) {
        ec = handleGetMessageResponse(collector);
        checkErrorCode(ec);
    }
    retryInterval = _transportAdapter->getRetryInterval(currentTime);
    if (retryInterval > 0 && !force) {
        return retryInterval;
    }

    if ((_nextTimestamp <= _timestampLimit && getUnReadMsgCount() < (int64_t)_config.readBufferSize) || force) {
        if (mayWaitForRetry(ec) && !force) {
            int64_t ci = currentTime - _partitionStatus.refreshTime;
            if (ci < (int64_t)_config.retryGetMsgInterval) {
                return _config.retryGetMsgInterval - ci;
            }
        }
        ec = postGetMessageRequest(collector);
        if (ec == ERROR_NONE) {
            isSent = true;
        }
        checkErrorCode(ec);
        retryInterval = _transportAdapter->getRetryInterval(currentTime);
        if (retryInterval > 0) {
            return retryInterval;
        }
    }
    return MAX_TIME_STAMP;
}

void SwiftSinglePartitionReader::checkErrorCode(ErrorCode ec) {
    if (ec != ERROR_NONE) {
        _lastErrorCode = ec;
        _errorCodeForCheck = ec;
    }
}

void SwiftSinglePartitionReader::resetLastErrorCode() { _lastErrorCode = ERROR_NONE; }

void SwiftSinglePartitionReader::checkCurrentError(ErrorCode &errorCode, string &errorMsg) const {
    errorCode = hasFatalError(_errorCodeForCheck);
    if (ERROR_NONE == errorCode) {
        return;
    }
    stringstream ss;
    int64_t interval = TimeUtility::currentTime() - _lastSuccessResponseTime;
    ss << "The interval between last Success getMsgReponse and now is:" << interval / 1000000 << " s"
       << ", topic name[" << _config.topicName << "], partitionId[" << _partitionId << "]"
       << ", last errorCode[" << _errorCodeForCheck << "].";
    errorMsg = ss.str();
}

ErrorCode SwiftSinglePartitionReader::hasFatalError(ErrorCode ec) const {
    bool isFatalError = (ec != ERROR_NONE) && (ec != ERROR_BROKER_SOME_MESSAGE_LOST) && (ec != ERROR_BROKER_NO_DATA) &&
                        (ec != ERROR_CLIENT_NO_MORE_MESSAGE);
    if (!isFatalError) {
        return ERROR_NONE;
    }

    int64_t interval = TimeUtility::currentTime() - _lastSuccessResponseTime;
    if (interval >= (int64_t)_config.fatalErrorTimeLimit) {
        return ec;
    } else {
        return ERROR_NONE;
    }
}

int64_t SwiftSinglePartitionReader::getUnReadMsgCount() { return _buffer.getUnReadMsgCount(); }

bool SwiftSinglePartitionReader::hasUnReadMsg() {
    if (getUnReadMsgCount() <= 0) {
        return false;
    } else if (getFirstMsgTimestamp() <= _timestampLimit) {
        return true;
    } else {
        return false;
    }
}

ErrorCode SwiftSinglePartitionReader::reportFatalError(bool resetLastError) {
    if (ERROR_SEALED_TOPIC_READ_FINISH == _lastErrorCode) {
        return _lastErrorCode;
    }
    ErrorCode ec = hasFatalError(_lastErrorCode);
    if (ec == ERROR_NONE) {
        return ERROR_NONE;
    }
    int64_t currentTime = TimeUtility::currentTime();
    int64_t interval = currentTime - _lastReportErrorTime;
    if (interval >= (int64_t)_config.fatalErrorReportInterval) {
        if (resetLastError) {
            _lastReportErrorTime = currentTime;
            resetLastErrorCode();
        }
        return ec;
    }
    return ERROR_NONE;
}

ErrorCode SwiftSinglePartitionReader::seekByMessageId(int64_t msgId) {
    if (msgId < 0) {
        AUTIL_LOG(WARN, "msgId[%ld] less than zero, set msgId to zero", msgId);
        msgId = 0;
    }
    ErrorCode ec = _transportAdapter->ignoreLastResponse();
    checkErrorCode(ec);
    _nextMsgId = _buffer.seek(msgId);
    _nextTimestamp = -1;
    _lastMsgTimestamp = -1;
    _lastMsgOffsetInMerge = 0;
    _lastCheckpointTimestamp = -1;
    _seekTimestamp = -1;
    _sealedTopicReadFinish = false;
    return ERROR_NONE;
}

ErrorCode SwiftSinglePartitionReader::seekByTimestamp(int64_t timestamp) {
    int64_t currTime = TimeUtility::currentTime();
    if (ERROR_NONE != _seekByTimestampRecord.lastErrorCode &&
        currTime < _seekByTimestampRecord.lastSeekTime + SEEKBYTIMESTAMP_ERROR_INTERVAL) {
        usleep(_seekByTimestampRecord.lastSeekTime + SEEKBYTIMESTAMP_ERROR_INTERVAL - currTime);
    }
    ErrorCode ec = doSeekByTimestamp(timestamp);
    if (ec == ERROR_NONE) {
        _seekTimestamp = timestamp;
    } else {
        AUTIL_LOG(WARN,
                  "[%s %d] seek to[%ld] error[%s]",
                  _config.topicName.c_str(),
                  _partitionId,
                  timestamp,
                  ErrorCode_Name(ec).c_str());
    }
    _seekByTimestampRecord.lastErrorCode = ec;
    _seekByTimestampRecord.lastSeekTime = TimeUtility::currentTime();
    return ec;
}

ErrorCode SwiftSinglePartitionReader::doSeekByTimestamp(int64_t timestamp) {
    int64_t msgid = -1;
    int64_t msgTime = -1;
    ErrorCode ec = getMinMessageIdByTime(timestamp, msgid, msgTime);
    AUTIL_LOG(DEBUG,
              "[%s %d] do seek to [%ld], error [%s], msgid [%ld], timestamp[%ld]",
              _config.topicName.c_str(),
              _partitionId,
              timestamp,
              ErrorCode_Name(ec).c_str(),
              msgid,
              msgTime);
    if (ec != ERROR_NONE) {
        checkErrorCode(ec);
        return ec;
    }
    ec = seekByMessageId(msgid);
    _nextTimestamp = msgTime;
    return ec;
}

ErrorCode SwiftSinglePartitionReader::getMinMessageIdByTime(int64_t timestamp, int64_t &msgid, int64_t &msgTime) {
    MessageIdRequest *request = new MessageIdRequest;
    request->set_timestamp(timestamp);
    ErrorCode ec = _msgIdTransportAdapter->postRequest(request);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(WARN,
                  "[%s %d] seek by timestamp failed for[%s]!",
                  _config.topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        delete request;
        return ec;
    }

    _msgIdTransportAdapter->waitLastRequestDone();
    MessageIdResponse *response = NULL;
    ec = _msgIdTransportAdapter->stealResponse(response);
    unique_ptr<MessageIdResponse> responsePtr(response);

    // handle session change or partition not found, maybe caused by partition changed
    HANDLE_SESSION_OR_PARTITION_CHANGE;
    if (ec != ERROR_NONE && ec != ERROR_BROKER_TIMESTAMP_TOO_LATEST && ec != ERROR_BROKER_NO_DATA) {
        AUTIL_LOG(WARN,
                  "[%s %d] getMinMessageIdByTime failed for [%s]!",
                  _config.topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        return ec;
    }
    assert(response);
    ErrorCode ec2 = fillPartitionInfo(_msgIdTransportAdapter->getLastTransportClosureDoneTime(), *response);
    if (ec2 != ERROR_NONE) {
        return ec2;
    }
    if (ec == ERROR_NONE) {
        msgid = response->msgid();
        msgTime = response->timestamp();
    } else if (ec == ERROR_BROKER_NO_DATA) {
        msgid = 0;
        msgTime = -1;
    } else if (ec == ERROR_BROKER_TIMESTAMP_TOO_LATEST) {
        msgid = response->maxmsgid() + 1;
        msgTime = std::max(response->maxtimestamp() + 1, timestamp);
    } else {
        assert(false);
    }
    return ERROR_NONE;
}

int64_t SwiftSinglePartitionReader::getFirstMsgTimestamp() { return _buffer.getFirstMsgTimestamp(); }

int64_t SwiftSinglePartitionReader::getNextMsgTimestamp() {
    int64_t f = getFirstMsgTimestamp();
    if (-1 != f) {
        return f;
    }
    return std::max(_nextTimestamp, _seekTimestamp);
}

std::pair<int64_t, int32_t> SwiftSinglePartitionReader::getCheckpointTimestamp() {
    int64_t checkpoint = getFirstMsgTimestamp(); // merge message has same msg timestamp
    int32_t offsetInMerge = 0;
    if (checkpoint == -1) { // read buffer no message
        if (_checkpointReflushMode) {
            checkpoint =
                std::max(_lastMsgTimestamp + 1, int64_t(_nextTimestamp - _config.checkpointRefreshTimestampOffset));
            checkpoint = std::max(checkpoint, _seekTimestamp);
        } else {
            checkpoint = std::max(_lastMsgTimestamp + 1, _seekTimestamp);
        }
    } else if (_lastMsgTimestamp == checkpoint) { // merge message
        if (_lastMsgOffsetInMerge < common::OFFSET_IN_MERGE_MSG_BASE) {
            AUTIL_LOG(
                WARN,
                "[%s %d] checkpoint timestamp [%ld] is same, but last msg offset in merge is [%d], less than [%d]. ",
                _config.topicName.c_str(),
                _partitionId,
                checkpoint,
                _lastMsgOffsetInMerge,
                common::OFFSET_IN_MERGE_MSG_BASE);
        } else {
            offsetInMerge = _lastMsgOffsetInMerge - common::OFFSET_IN_MERGE_MSG_BASE + 1;
        }
    }

    if (checkpoint < _lastCheckpointTimestamp) {
        AUTIL_LOG(WARN,
                  "[%s %d] checkpoint timestamp roll back, current checkpoint timestamp [%ld], last checkpoint "
                  "timestamp[%ld]",
                  _config.topicName.c_str(),
                  _partitionId,
                  checkpoint,
                  _lastCheckpointTimestamp);
    }
    _lastCheckpointTimestamp = checkpoint;
    return std::make_pair(_lastCheckpointTimestamp, offsetInMerge);
}

ErrorCode SwiftSinglePartitionReader::handleGetMessageResponse(ClientMetricsCollector &collector) {
    MessageResponse *response = NULL;
    ErrorCode ec = _transportAdapter->stealResponse(response);
    if (!response) {
        AUTIL_LOG(WARN, "[%s %d] Invalid broker response, response is empty", _config.topicName.c_str(), _partitionId);
        return ec;
    }
    if (response->requestuuid() == 0) {
        response->set_requestuuid(_curRequestUuid);
    } else if (response->requestuuid() != _curRequestUuid) {
        AUTIL_LOG(INFO,
                  "[%s %d] request uuid not match expect [%lu] actual [%lu]",
                  _config.topicName.c_str(),
                  _partitionId,
                  _curRequestUuid,
                  response->requestuuid());
    }

    unique_ptr<MessageResponse> auto_response(response);
    if (ERROR_BROKER_INVALID_USER == ec) {
        AUTIL_LOG(WARN,
                  "username[%s] not permitter to read[%s %d]",
                  _transportAdapter->getUsername().c_str(),
                  _config.topicName.c_str(),
                  _partitionId);
        return ec;
    }
    AUTIL_LOG(DEBUG, "handle [%s %d %ld] response", _config.topicName.c_str(), _partitionId, response->requestuuid());
    // handle session change or partition not found, maybe caused by partition changed
    HANDLE_SESSION_OR_PARTITION_CHANGE;

    if (ec != ERROR_NONE && ec != ERROR_BROKER_NO_DATA && ec != ERROR_BROKER_SOME_MESSAGE_LOST &&
        ec != ERROR_SEALED_TOPIC_READ_FINISH) {
        AUTIL_LOG(WARN,
                  "getMessage from [%s %d %ld] failed for [%s]!",
                  _config.topicName.c_str(),
                  _partitionId,
                  response->requestuuid(),
                  ErrorCode_Name(ec).c_str());
        return ec;
    }
    if (ERROR_BROKER_SOME_MESSAGE_LOST == ec) {
        AUTIL_LOG(WARN,
                  "some message lost in[%s %d, %ld]! maybe broker read file failed",
                  _config.topicName.c_str(),
                  _partitionId,
                  response->requestuuid());
    }
    assert(response);
    if (response->has_sessionid()) {
        _sessionId = response->sessionid();
    }
    if (ERROR_SEALED_TOPIC_READ_FINISH == ec && 0 == _buffer.getUnReadMsgCount()) {
        _sealedTopicReadFinish = true;
        AUTIL_LOG(INFO,
                  "[%s:%d %ld] read finsh[%s]",
                  _config.topicName.c_str(),
                  _partitionId,
                  response->requestuuid(),
                  ErrorCode_Name(ec).c_str());
        return ec;
    }
    ErrorCode ec2;
    ec2 = fillPartitionInfo(_transportAdapter->getLastTransportClosureDoneTime(), *response);
    if (ec2 != ERROR_NONE) {
        return ec2;
    }
    if (!isValidateResponse(response, ec)) {
        return ERROR_CLIENT_INVALID_RESPONSE;
    }
    _lastSuccessResponseTime = TimeUtility::currentTime();
    int64_t maxMsgId = response->maxmsgid();
    uint32_t msgSize = 0;
    int64_t lastTimeStamp = -1;
    if (response->messageformat() == MF_FB) {
        protocol::FBMessageReader reader;
        if (!reader.init(response->fbmsgs(), false)) {
            return ERROR_CLIENT_INVALID_RESPONSE;
        }
        msgSize = reader.size();
        if (msgSize > 0) {
            lastTimeStamp = (reader.read(msgSize - 1))->timestamp();
        }
    } else {
        msgSize = response->msgs_size();
        if (msgSize > 0) {
            lastTimeStamp = (response->msgs()[msgSize - 1]).timestamp();
        }
    }
    if (response->has_maxtimestamp() && -1 != lastTimeStamp) {
        if (_metricsReporter) {
            ReaderDelayCollector delayCollector;
            delayCollector.readDelay = response->maxtimestamp() - lastTimeStamp;
            delayCollector.currentDelay = TimeUtility::currentTime() - lastTimeStamp;
            _metricsReporter->reportDelay(delayCollector, &_metricsTags);
        }
    }
    collector.requestMsgCount = msgSize;
    if (ec == ERROR_BROKER_NO_DATA) {
        ec = ERROR_CLIENT_NO_MORE_MESSAGE;
    } else if (msgSize > 0) {
        if (!_buffer.addResponse(auto_response.release())) {
            string errInfo = "[" + _config.topicName + " " + StringUtil::toString(_partitionId) + " " +
                             StringUtil::toString(response->requestuuid()) +
                             "] add response to read buffer failed. has msg count: " + StringUtil::toString(msgSize);
            AUTIL_LOG(ERROR, "%s", errInfo.c_str());
            ec = ERROR_CLIENT_INVALID_RESPONSE;
            auto *errCollector = ErrorCollectorSingleton::getInstance();
            errCollector->addRequestHasErrorDataInfo(errInfo);
            auto *responseCollector = ResponseCollectorSingleton::getInstance();
            string content;
            if (response->SerializeToString(&content)) {
                responseCollector->logResponse(_readerInfo, MESSAGE_RESPONSE, content);
            } else {
                responseCollector->logResponse(_readerInfo, MESSAGE_RESPONSE_FBMSG_PART, response->fbmsgs());
            }
            // return ec; // TODO, retry or skip request bug
        }
        _nextMsgId = response->nextmsgid();
    } else {
        if (_nextMsgId <= maxMsgId) {
            _nextMsgId = response->nextmsgid();
        } else {
            int64_t oldNextId = _nextMsgId;
            _nextMsgId = maxMsgId + 1;
            if (_nextMsgId < oldNextId) {
                AUTIL_LOG(WARN,
                          "response [%ld], Next msgId rollback from [%ld] to [%ld]",
                          response->requestuuid(),
                          oldNextId,
                          _nextMsgId);
            }
            ec = ERROR_CLIENT_NO_MORE_MESSAGE;
        }
    }
    if (response->has_nexttimestamp()) {
        _nextTimestamp = response->nexttimestamp();
    }
    return ec;
}

bool SwiftSinglePartitionReader::isValidateResponse(MessageResponse *response, ErrorCode ec) const {
    if (ec != ERROR_BROKER_NO_DATA) {
        if (!response->has_nextmsgid()) {
            return false;
        }
        if (ec != ERROR_BROKER_SOME_MESSAGE_LOST && !response->has_nexttimestamp()) {
            return false;
        }
    }
    return true;
}

ErrorCode SwiftSinglePartitionReader::postGetMessageRequest(ClientMetricsCollector &collector) {
    ConsumptionRequest *request = new ConsumptionRequest;
    request->set_startid(_nextMsgId);
    request->set_sessionidextend(_sessionId);
    request->set_identitystr(_config.readerIdentity);
    _requestSeq++;
    int64_t curTime = TimeUtility::currentTime();
    _curRequestUuid =
        SwiftUuidGenerator::genRequestUuid(curTime / 1000, _partitionId, SwiftClient::traceFlag, _requestSeq);
    request->set_requestuuid(_curRequestUuid);
    request->set_generatedtime(curTime);
    if (_nextTimestamp != -1) {
        request->set_starttimestamp(_nextTimestamp);
    }
    if (_clientCommittedCheckpoint != -1) {
        request->set_committedcheckpoint(_clientCommittedCheckpoint);
    }
    request->set_count(_config.oneRequestReadCount);
    request->set_maxtotalsize(_config.consumptionRequestBytesLimit);
    if (!_config.readAll) {
        *request->mutable_filter() = _config.swiftFilter;
    }
    ClientVersionInfo *clientVersionInfo = request->mutable_versioninfo();
    *clientVersionInfo = _clientVersionInfo;
    for (vector<string>::const_iterator it = _requiredFieldNames.begin(); it != _requiredFieldNames.end(); ++it) {
        request->add_requiredfieldnames(*it);
    }
    if (!_filedFilterDesc.empty()) {
        request->set_fieldfilterdesc(_filedFilterDesc);
    }
    request->set_needcompress(_config.needCompress);
    request->set_candecompressmsg(_config.canDecompress);
    AUTIL_LOG(DEBUG,
              "[%s %d] post read request is [%s]",
              _config.topicName.c_str(),
              _partitionId,
              request->ShortDebugString().c_str());
    ErrorCode ec = _transportAdapter->postRequest(request, common::DEFAULT_POST_TIMEOUT, &collector);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(WARN,
                  "post getMessageRequest to[%s %d] failed, ec[%s]",
                  _config.topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        collector.reset(true);
        DELETE_AND_SET_NULL(request);
        if (ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER == ec) {
            _func(_lastTopicVersion);
        }
    }
    if (_metricsReporter) {
        _metricsReporter->reportReaderMetrics(collector, &_metricsTags);
    }
    return ec;
}

template <typename ResponseType>
ErrorCode SwiftSinglePartitionReader::fillPartitionInfo(int64_t refreshTime, const ResponseType &response) {
    ScopedWriteLock wlock(_partStatusLock);
    _partitionStatus.refreshTime = refreshTime;
    ErrorCode ec = response.errorinfo().errcode();

    if (ec == ERROR_BROKER_NO_DATA) {
        _partitionStatus.maxMessageId = -1;
        _partitionStatus.maxMessageTimestamp = -1;
        return ERROR_NONE;
    }

    if (!response.has_maxmsgid() || !response.has_maxtimestamp()) {
        AUTIL_LOG(WARN, "[%s %d] invalid response!", _config.topicName.c_str(), _partitionId);
        return ERROR_CLIENT_INVALID_RESPONSE;
    }

    _partitionStatus.maxMessageId = response.maxmsgid();
    _partitionStatus.maxMessageTimestamp = response.maxtimestamp();
    return ERROR_NONE;
}

SwiftPartitionStatus SwiftSinglePartitionReader::getSwiftPartitionStatus(int64_t curTime) {
    if (!_maxMsgIdTransportAdapter->isLastRequestDone()) {
        ScopedReadLock rlock(_partStatusLock);
        return _partitionStatus;
    }
    if (!_maxMsgIdTransportAdapter->isLastRequestHandled()) {
        handleGetMessageIdResponse();
    }
    ScopedReadLock rlock(_partStatusLock);
    int64_t refreshInterval = _config.partitionStatusRefreshInterval;
    if (curTime - _partitionStatus.refreshTime > refreshInterval && _lastRefreshMsgIdTime < curTime) {
        MessageIdRequest *request = new MessageIdRequest;
        _lastRefreshMsgIdTime = curTime;
        int64_t requestUuid = SwiftUuidGenerator::genRequestUuid(
            TimeUtility::currentTime() / 1000, _partitionId, SwiftClient::traceFlag, _requestSeq);
        request->set_requestuuid(requestUuid);
        ErrorCode ec = _maxMsgIdTransportAdapter->postRequest(request);
        if (ERROR_NONE != ec) {
            AUTIL_LOG(INFO,
                      "[%s %d] post getMaxMessageIdRequest failed, ec[%s]",
                      _config.topicName.c_str(),
                      _partitionId,
                      ErrorCode_Name(ec).c_str());
            delete request;
            if (ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER == ec) {
                _func(_lastTopicVersion);
            }
        }
    }
    return _partitionStatus;
}

ErrorCode SwiftSinglePartitionReader::handleGetMessageIdResponse() {
    MessageIdResponse *response = NULL;
    ErrorCode ec = _maxMsgIdTransportAdapter->stealResponse(response);
    assert(response);
    unique_ptr<MessageIdResponse> responsePtr(response);
    // handle session change or partition not found, maybe caused by partition changed
    HANDLE_SESSION_OR_PARTITION_CHANGE;

    ScopedWriteLock wlock(_partStatusLock);
    if (ERROR_BROKER_NO_DATA == ec) {
        _partitionStatus.refreshTime = _lastRefreshMsgIdTime;
        _partitionStatus.maxMessageId = -1;
        _partitionStatus.maxMessageTimestamp = -1;
        return ec;
    }
    if (ERROR_NONE != ec) {
        AUTIL_LOG(INFO,
                  "[%s %d] getMaxMessageId failed for [%s]!",
                  _config.topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        return ec;
    }
    _partitionStatus.refreshTime = _lastRefreshMsgIdTime;
    _partitionStatus.maxMessageId = response->msgid();
    _partitionStatus.maxMessageTimestamp = response->timestamp();
    return ec;
}

void SwiftSinglePartitionReader::setRequiredFieldNames(const vector<string> &fieldNames) {
    _requiredFieldNames = fieldNames;
    _buffer.updateFilter(_config.swiftFilter, _requiredFieldNames, _filedFilterDesc);
    ErrorCode ec = _transportAdapter->ignoreLastResponse();
    checkErrorCode(ec);
    Message msg;
    if (!_buffer.read(msg)) {
        return;
    }
    seekByMessageId(msg.msgid());
}

vector<string> SwiftSinglePartitionReader::getRequiredFieldNames() { return _requiredFieldNames; }

void SwiftSinglePartitionReader::setDecompressThreadPool(autil::ThreadPoolPtr decompressThreadPool) {
    _buffer.setDecompressThreadPool(decompressThreadPool);
}

void SwiftSinglePartitionReader::setFieldFilterDesc(const std::string &fieldFilterDesc) {
    _filedFilterDesc = fieldFilterDesc;
    _buffer.updateFilter(_config.swiftFilter, _requiredFieldNames, _filedFilterDesc);
    ErrorCode ec = _transportAdapter->ignoreLastResponse();
    checkErrorCode(ec);
    Message msg;
    if (!_buffer.read(msg)) {
        return;
    }
    seekByMessageId(msg.msgid());
}

string SwiftSinglePartitionReader::getFieldFilterDesc() { return _filedFilterDesc; }

void SwiftSinglePartitionReader::setTimestampLimit(int64_t timestampLimit) {
    assert(timestampLimit >= _lastMsgTimestamp);
    _timestampLimit = timestampLimit;
}

int64_t SwiftSinglePartitionReader::getTimestampLimit() const { return _timestampLimit; }
int64_t SwiftSinglePartitionReader::getLastMsgTimestamp() { return _lastMsgTimestamp; }

void SwiftSinglePartitionReader::setCallBackFunc(const std::function<protocol::ErrorCode(int64_t)> &func) {
    _func = func;
}

bool SwiftSinglePartitionReader::updateCommittedCheckpoint(int64_t checkpoint) {
    if (_clientCommittedCheckpoint > checkpoint) {
        AUTIL_LOG(WARN, "update checkpoint [%ld] is small than current [%ld].", checkpoint, _clientCommittedCheckpoint);
        return false;
    }
    _clientCommittedCheckpoint = checkpoint;
    bool isSent = false;
    tryFillBuffer(autil::TimeUtility::currentTime(), true, isSent);
    if (isSent) {
        AUTIL_LOG(INFO,
                  "send update checkpoint [%ld] to [%s %d] success.",
                  checkpoint,
                  _config.topicName.c_str(),
                  _partitionId);
        return true;
    } else {
        AUTIL_LOG(WARN,
                  "send update checkpoint [%ld] to [%s %d] fail, will update in next read.",
                  checkpoint,
                  _config.topicName.c_str(),
                  _partitionId);
        return false;
    }
}

bool SwiftSinglePartitionReader::mayWaitForRetry(ErrorCode ec) {
    ScopedReadLock rlock(_partStatusLock);
    return (_nextMsgId > _partitionStatus.maxMessageId &&
            (!_isTopicLongPollingEnabled ||
             (_isTopicLongPollingEnabled && ERROR_NONE != ec && ec != ERROR_CLIENT_NO_MORE_MESSAGE)));
}

std::pair<uint32_t, uint32_t> SwiftSinglePartitionReader::getFilterRange() const {
    return std::make_pair(_rangeFrom, _rangeTo);
}

void SwiftSinglePartitionReader::setFilterRange(uint32_t from, uint32_t to) {
    _rangeFrom = from;
    _rangeTo = to;
}

} // namespace client
} // namespace swift
