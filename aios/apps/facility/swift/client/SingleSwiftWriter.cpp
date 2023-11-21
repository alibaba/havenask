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
#include "swift/client/SingleSwiftWriter.h"

#include <assert.h>
#include <cstdint>
#include <iosfwd>
#include <limits>
#include <memory>
#include <stdlib.h>
#include <string>
#include <unistd.h>

#include "autil/CommonMacros.h"
#include "autil/Singleton.h"
#include "autil/TimeUtility.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/SwiftTransportAdapter.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/config/ClientAuthorizerInfo.h"
#include "swift/monitor/ClientMetricsReporter.h"
#include "swift/monitor/MetricsCommon.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/MessageCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"
#include "swift/util/SwiftUuidGenerator.h"
#include "swift/version.h"

namespace swift {
namespace common {
class MessageInfo;
} // namespace common
} // namespace swift

using namespace std;
using namespace autil;
using namespace flatbuffers;
using namespace swift::common;
using namespace swift::util;
using namespace swift::protocol;
using namespace swift::network;
using namespace swift::monitor;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SingleSwiftWriter);

int64_t SingleSwiftWriter::INVALID_CHECK_POINT_ID = std::numeric_limits<int64_t>::min();

SingleSwiftWriter::SingleSwiftWriter(SwiftAdminAdapterPtr adminAdapter,
                                     SwiftRpcChannelManagerPtr channelManager,
                                     uint32_t partitionId,
                                     const SwiftWriterConfig &config,
                                     BlockPoolPtr blockPool,
                                     autil::ThreadPoolPtr mergeThreadPool,
                                     BufferSizeLimiterPtr limiter,
                                     monitor::ClientMetricsReporter *reporter,
                                     int64_t topicVersion,
                                     int64_t sessionId)
    : _transportAdapter(
          new SwiftTransportAdapter<TRT_SENDMESSAGE>(adminAdapter, channelManager, config.topicName, partitionId))
    , _partitionId(partitionId)
    , _config(config)
    , _lastSendRequestTime(TimeUtility::currentTime())
    , _checkpointId(INVALID_CHECK_POINT_ID)
    , _refreshTime(-1)
    , _retreatNextTimestamp(-1)
    , _lastErrorCode(ERROR_NONE)
    , _writeBuffer(blockPool, mergeThreadPool, limiter, config.oneRequestSendByteCount)
    , _sessionId(sessionId)
    , _topicChangeFunc(NULL)
    , _committedFunc(NULL)
    , _blockPool(blockPool)
    , _lastTopicVersion(topicVersion)
    , _hasVersionError(false)
    , _metricsReporter(reporter) {

    protocol::AuthenticationInfo authInfo;
    *authInfo.mutable_accessauthinfo() = config.getAccessAuthInfo();
    if (adminAdapter) {
        auto clientAuthInfo = adminAdapter->getAuthenticationConf();
        if (!clientAuthInfo.isEmpty()) {
            authInfo.set_username(clientAuthInfo.username);
            authInfo.set_passwd(clientAuthInfo.passwd);
        }
    }
    _transportAdapter->setAuthInfo(authInfo);
    _clientVersionInfo.set_version(swift_client_version_str);
    _clientVersionInfo.set_supportfb(true);
    _clientVersionInfo.set_clienttype(CT_CPP);
    _clientVersionInfo.set_supportauthentication(true);
    _writeBuffer.setTopicName(config.topicName);
    _writeBuffer.setPartitionId(_partitionId);
    string idStr = _config.topicName + " " + to_string(_partitionId);
    _transportAdapter->setIdStr("write " + idStr);
    _metricsTags.AddTag("topic", config.topicName);
    _metricsTags.AddTag("partition", intToString(_partitionId));
}

SingleSwiftWriter::~SingleSwiftWriter() {
    DELETE_AND_SET_NULL(_transportAdapter);
    if (_writeBuffer.getSize() != 0) {
        AUTIL_LOG(WARN,
                  "[%s %u]  has [%d] message not sent, [%d] message not committed",
                  _config.topicName.c_str(),
                  _partitionId,
                  (int)_writeBuffer.getUnsendCount(),
                  (int)_writeBuffer.getUncommittedCount());
    }
}

ErrorCode SingleSwiftWriter::write(MessageInfo &msgInfo, bool isSynchronous) {
    if ((int64_t)msgInfo.data.size() > MAX_MESSAGE_SIZE) {
        AUTIL_LOG(WARN,
                  "[%s %u] message length[%ld] exceed MAX_MESSAGE_SIZE[%ld]",
                  _config.topicName.c_str(),
                  _partitionId,
                  msgInfo.data.size(),
                  MAX_MESSAGE_SIZE);
        return ERROR_BROKER_MSG_LENGTH_EXCEEDED;
    }
    if (_hasVersionError) {
        AUTIL_INTERVAL_LOG(100,
                           WARN,
                           "[%s %d] write version[%s %u-%u] invalid",
                           _config.topicName.c_str(),
                           _partitionId,
                           _config.writerName.c_str(),
                           _config.majorVersion,
                           _config.minorVersion);
        return ERROR_BROKER_WRITE_VERSION_INVALID;
    }
    if (isSynchronous) { // sync
        ScopedLock lock(_synchronousMutex);
        if (!_writeBuffer.addWriteMessage(msgInfo)) {
            return addMsgFail();
        }
        ClientMetricsCollector collector;
        sendSyncRequest(TimeUtility::currentTime(), _config.retryTimes, collector);
        ErrorCode ec = getLastErrorCode();
        return ec;
    }
    // async
    if (!_writeBuffer.addWriteMessage(msgInfo)) {
        return addMsgFail();
    }
    return ERROR_NONE;
}

ErrorCode SingleSwiftWriter::addMsgFail() {
    ErrorCode ec = getLastErrorCode();
    if (ec == ERROR_BROKER_BUSY || ec == ERROR_NONE) {
        size_t inWriteCount, toSendCount, leftMergeCount, uncommittedCount;
        _writeBuffer.getMsgInBufferInfo(inWriteCount, toSendCount, leftMergeCount, uncommittedCount);
        AUTIL_INTERVAL_LOG(100,
                           WARN,
                           "Partition [%s %u] Send buffer is full or merge queue not empty, "
                           "partition send buffer is [%lu], total use buffer[%ld], "
                           "uncommitted message count[%lu], unsend message count[%lu], "
                           "write queue count[%lu], left merge count[%lu]",
                           _config.topicName.c_str(),
                           _partitionId,
                           _writeBuffer.getSize(),
                           _blockPool->getBlockSize() * _blockPool->getUsedBlockCount(),
                           uncommittedCount,
                           toSendCount,
                           inWriteCount,
                           leftMergeCount);

        return ERROR_CLIENT_SEND_BUFFER_FULL;
    }
    AUTIL_MASSIVE_LOG(WARN,
                      "Partition [%s %u] write message failed, error code[%s], "
                      "send buffer is [%lu], total use buffer[%ld].",
                      _config.topicName.c_str(),
                      _partitionId,
                      ErrorCode_Name(ec).c_str(),
                      _writeBuffer.getSize(),
                      _blockPool->getBlockSize() * _blockPool->getMaxBlockCount());
    return ec;
}

bool SingleSwiftWriter::getCommittedCheckpointId(int64_t &checkpoint) {
    bool hasMsg = false;
    int64_t tmpCheckPoint;
    hasMsg = _writeBuffer.latestCheckPointId(tmpCheckPoint);
    ScopedLock lock(_mutex);
    if (hasMsg) {
        _checkpointId = tmpCheckPoint - 1;
    }
    checkpoint = _checkpointId;
    return hasMsg;
}

bool SingleSwiftWriter::getMaxCheckpointId(int64_t &checkpoint) { return _writeBuffer.maxCheckPointId(checkpoint); }

bool SingleSwiftWriter::isBufferEmpty() { return _writeBuffer.getQueueSize() == 0; }

bool SingleSwiftWriter::isSent() { return _writeBuffer.getUnsendCount() == 0; }

ErrorCode SingleSwiftWriter::sendRequest(int64_t now, bool force) {
    if (!_transportAdapter->isLastRequestDone()) {
        return ERROR_NONE;
    }
    ClientMetricsCollector collector;
    if (!_transportAdapter->isLastRequestHandled()) {
        handleSendMessageResponse();
    }
    if (_writeBuffer.getSealed()) {
        AUTIL_INTERVAL_LOG(100, WARN, "[%s %d] is sealed, cannot send msg", _config.topicName.c_str(), _partitionId);
        return ERROR_TOPIC_SEALED;
    }
    if (_hasVersionError) {
        AUTIL_INTERVAL_LOG(100, WARN, "[%s %d] write version invalid", _config.topicName.c_str(), _partitionId);
        if (_errorFunc) {
            collector.errorCallBack = true;
            _errorFunc(ERROR_BROKER_WRITE_VERSION_INVALID);
            if (_metricsReporter) {
                collector.unsendCount = getUnsendMessageCount();
                collector.uncommitCount = getUncommittedMsgCount();
                _metricsReporter->reportWriterMetrics(collector, &_metricsTags);
            }
        }
        return ERROR_BROKER_WRITE_VERSION_INVALID;
    }
    if (_lastErrorCode == ERROR_CLIENT_ARPC_ERROR) {
        collector.rpcError = true;
    }
    if (_writeBuffer.getUnsendSize() >= (size_t)_config.oneRequestSendByteCount ||
        now >= _lastSendRequestTime + (int64_t)_config.maxKeepInBufferTime || force == true) {
        if (_lastErrorCode != ERROR_NONE &&
            (now < _lastSendRequestTime + (int64_t)_config.retryTimeInterval || needRetreat(now))) {
            AUTIL_INTERVAL_LOG(100,
                               INFO,
                               "[%s %d] suspend send request for ec[%s],"
                               " last send [%ld]",
                               _config.topicName.c_str(),
                               _partitionId,
                               ErrorCode_Name(_lastErrorCode).c_str(),
                               _lastSendRequestTime);
            return _lastErrorCode;
        }
        if (postSendMessageRequest(now, collector, _config.asyncSendTimeout)) {
            return ERROR_NONE;
        }
    }

    return _lastErrorCode;
}

void SingleSwiftWriter::clearBuffer() { _writeBuffer.clear(); }

void SingleSwiftWriter::sendSyncRequest(int64_t now, uint32_t retryTimes, ClientMetricsCollector &collector) {
    assert(ERROR_NONE == _transportAdapter->ignoreLastResponse());
    uint32_t actualRetryTimes = 0;
    do {
        actualRetryTimes++;
        bool ret = postSendMessageRequest(now, collector, _config.syncSendTimeout);
        if (ret) {
            _transportAdapter->waitLastRequestDone();
            handleSendMessageResponse();
        }
        if (_lastErrorCode == ERROR_BROKER_SESSION_CHANGED ||
            _lastErrorCode == ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND) {
            AUTIL_LOG(INFO,
                      "[%s %d] session [%ld] changed, ec [%s]. ",
                      _config.topicName.c_str(),
                      _partitionId,
                      _sessionId,
                      ErrorCode_Name(_lastErrorCode).c_str());
            break;
        }
        if ((!ret) || ERROR_NONE != _lastErrorCode) {
            _writeBuffer.resetUnsendCursor();
            usleep(_config.retryTimeInterval);
        }
    } while ((_writeBuffer.getSize() > 0) && (actualRetryTimes < retryTimes));
    _writeBuffer.clear();
}

bool SingleSwiftWriter::needRetreat(int64_t currentTime) {
    if (ERROR_BROKER_BUSY != _lastErrorCode && ERROR_CLIENT_INVALID_RESPONSE != _lastErrorCode &&
        ERROR_CLIENT_ARPC_ERROR != _lastErrorCode && ERROR_BROKER_INVALID_USER != _lastErrorCode) {
        _retreatNextTimestamp = -1;
        return false;
    }
    if (ERROR_CLIENT_ARPC_ERROR == _lastErrorCode && !_transportAdapter->brokerAddressIsSameAsLast()) {
        _retreatNextTimestamp = -1;
        return false;
    }
    // first busy, so set next timestamp
    if (_retreatNextTimestamp < 0) {
        int64_t interval = 0;
        if (_config.brokerBusyWaitIntervalMin == _config.brokerBusyWaitIntervalMax) {
            interval = _config.brokerBusyWaitIntervalMin;
        } else {
            int64_t seed = TimeUtility::currentTime();
            srand(seed);
            interval = _config.brokerBusyWaitIntervalMin +
                       (rand() % (_config.brokerBusyWaitIntervalMax - _config.brokerBusyWaitIntervalMin + 1));
        }
        _retreatNextTimestamp = _lastSendRequestTime + interval;
        return true;
    }
    if (currentTime >= _retreatNextTimestamp) {
        _retreatNextTimestamp = -1;
        return false;
    }
    return true;
}

bool SingleSwiftWriter::postSendMessageRequest(int64_t now, ClientMetricsCollector &collector, int64_t timeout) {
    if (_writeBuffer.getUnsendCount() == 0) {
        if (!needDetectCommitProgress(now)) {
            return true;
        } else {
            collector.isDetectCommit = true;
            AUTIL_INTERVAL_LOG(5000,
                               INFO,
                               "[%s %d] detect commit progress."
                               " uncommitted count [%d]",
                               _config.topicName.c_str(),
                               (int)_partitionId,
                               (int)_writeBuffer.getUncommittedCount());
        }
    }
    _lastSendRequestTime = now;
    ProductionRequest *request = new ProductionRequest;
    request->set_compressmsginbroker(_config.compressMsgInBroker);
    request->set_sessionid(_sessionId);
    request->set_needtimestamp(_config.needTimestamp);
    request->set_identitystr(_config.writerIdentity);
    _requestSeq++;
    int64_t curTime = TimeUtility::currentTime();
    int64_t curRequestUuid =
        SwiftUuidGenerator::genRequestUuid(curTime / 1000, _partitionId, SwiftClient::traceFlag, _requestSeq);
    request->set_requestuuid(curRequestUuid);
    request->set_generatedtime(curTime);
    request->set_writername(_config.writerName);
    request->set_majorversion(_config.majorVersion);
    request->set_minorversion(_config.minorVersion);
    ClientVersionInfo *clientVersionInfo = request->mutable_versioninfo();
    *clientVersionInfo = _clientVersionInfo;
    BrokerVersionInfo versionInfo = _transportAdapter->getBrokerVersionInfo();
    AUTIL_LOG(DEBUG,
              "[%s %u] broker version info [%s]",
              _config.topicName.c_str(),
              _partitionId,
              versionInfo.ShortDebugString().c_str());
    if (!versionInfo.supportmergemsg()) {
        _writeBuffer.updateMergeInfo(
            false, _config.mergeThresholdInCount, _config.mergeThresholdInSize, _config.compressThresholdInBytes);
    } else {
        _writeBuffer.updateMergeInfo(_config.mergeMsg,
                                     _config.mergeThresholdInCount,
                                     _config.mergeThresholdInSize,
                                     _config.compressThresholdInBytes);
    }
    if (versionInfo.supportfb() && _config.messageFormat == 1) {
        ThreadBasedObjectPool<FBMessageWriter> *objectPool =
            Singleton<ThreadBasedObjectPool<FBMessageWriter>>::getInstance();
        FBMessageWriter *writer = objectPool->getObject();
        writer->reset();
        collector.requestMsgCount = 0;
        _writeBuffer.fillRequest(writer, getOneRequestSendBytes(), collector.requestMsgCount);
        writer->finish();
        request->set_fbmsgs(writer->data(), writer->size());
        request->set_messageformat(MF_FB);
    } else {
        _writeBuffer.fillRequest(*request, getOneRequestSendBytes());
        request->set_messageformat(MF_PB);
        collector.requestMsgCount = request->msgs_size();
    }
    if (_config.compress) {
        MessageCompressor::compressProductionRequest(request);
    }
    AUTIL_LOG(DEBUG,
              "[%s %d]send msg count [%d], request is [%s]",
              _config.topicName.c_str(),
              _partitionId,
              (int)request->msgs_size(),
              request->ShortDebugString().c_str());

    auto ec = _transportAdapter->postRequest(request, timeout, &collector);
    bool ret = true;
    if (ec != ERROR_NONE) {
        AUTIL_LOG(WARN,
                  "[%s %u] Post sendMessageRequest failed, ec[%s]",
                  _config.topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        setLastErrorCode(ec);
        collector.reset(true);
        DELETE_AND_SET_NULL(request);
        ret = false;
    }
    if (_metricsReporter) {
        collector.unsendCount = getUnsendMessageCount();
        collector.uncommitCount = getUncommittedMsgCount();
        collector.usedBlockCount = _blockPool->getUsedBlockCount();
        auto blockSize = _blockPool->getBlockSize();
        collector.usedTotalPoolSize = blockSize * collector.usedBlockCount;
        collector.totalPoolSize = blockSize * (collector.usedBlockCount + _blockPool->getUnusedBlockCount());
        _metricsReporter->reportWriterMetrics(collector, &_metricsTags);
    }
    return ret;
}

size_t SingleSwiftWriter::getUnsendMessageCount() { return _writeBuffer.getUnsendCount(); }

size_t SingleSwiftWriter::getUnsendMessageSize() { return _writeBuffer.getUnsendSize(); }

size_t SingleSwiftWriter::getUncommittedMsgCount() { return _writeBuffer.getUncommittedCount(); }

size_t SingleSwiftWriter::getUncommittedMsgSize() { return _writeBuffer.getUncommittedSize(); }

bool SingleSwiftWriter::needDetectCommitProgress(int64_t now) {
    return _config.safeMode && now >= _lastSendRequestTime + (int64_t)_config.commitDetectionInterval &&
           _writeBuffer.getUncommittedCount() > 0;
}

void SingleSwiftWriter::handleSendMessageResponse() {
    MessageResponse *response = NULL;
    ErrorCode ec = _transportAdapter->stealResponse(response);
    std::unique_ptr<MessageResponse> responsePtr(response);
    if (response) {
        AUTIL_LOG(DEBUG,
                  "handle [%s %d] response[%s]",
                  _config.topicName.c_str(),
                  _partitionId,
                  response->ShortDebugString().c_str());
    }
    setLastErrorCode(ec);
    // invalid response
    if (!response) {
        AUTIL_LOG(WARN, "[%s %d] Invalid broker response, response is empty", _config.topicName.c_str(), _partitionId);
        return;
    }
    if (ERROR_BROKER_INVALID_USER == ec) {
        AUTIL_LOG(WARN,
                  "username[%s] not permitter to write[%s %d]",
                  _transportAdapter->getUsername().c_str(),
                  _config.topicName.c_str(),
                  _partitionId);
        return;
    }
    if (_lastTopicVersion == -1 && response->has_topicversion()) {
        _lastTopicVersion = response->topicversion();
    }
    if (ec == ERROR_BROKER_SESSION_CHANGED || ec == ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND) {
        _writeBuffer.resetUnsendCursor();
        if (_topicChangeFunc) {
            int64_t topicVersion = 0;
            ErrorCode funcEc;
            if (response->has_topicversion()) {
                topicVersion = response->topicversion();
            }
            if (topicVersion > 0 || _lastTopicVersion > 0) { // topicversion is empty when partiton not load
                if (topicVersion != _lastTopicVersion) {
                    if (topicVersion != 0) {
                        funcEc = _topicChangeFunc(topicVersion);
                    } else {
                        funcEc = _topicChangeFunc(_lastTopicVersion);
                    }
                    if (funcEc != ERROR_NONE) {
                        AUTIL_LOG(WARN,
                                  "[%s %d] set topic change failed, ec [%s]",
                                  _config.topicName.c_str(),
                                  _partitionId,
                                  ErrorCode_Name(funcEc).c_str());
                        return;
                    }
                    if (topicVersion != 0) {
                        AUTIL_LOG(INFO,
                                  "[%s %d] topic version changed from [%ld] to [%ld]",
                                  _config.topicName.c_str(),
                                  _partitionId,
                                  _lastTopicVersion,
                                  topicVersion);
                        _lastTopicVersion = topicVersion;
                    }
                }
            } else { // compatible old server
                AUTIL_LOG(INFO, "[%s %d] topic version may changed.", _config.topicName.c_str(), _partitionId);
                funcEc = _topicChangeFunc(topicVersion);
                if (funcEc != ERROR_NONE) {
                    AUTIL_LOG(WARN,
                              "[%s %d]set topic change failed, ec [%s]",
                              _config.topicName.c_str(),
                              _partitionId,
                              ErrorCode_Name(funcEc).c_str());
                    return;
                }
            }
        }
        if (response->has_sessionid()) { // session changed
            AUTIL_LOG(INFO,
                      "[%s %d] sessionid changed from [%ld] to [%ld]",
                      _config.topicName.c_str(),
                      _partitionId,
                      _sessionId,
                      response->sessionid());
            _sessionId = response->sessionid();
        }
        return;
    }
    if (ec == ERROR_TOPIC_SEALED) {
        _writeBuffer.setSealed(true);
        return;
    }
    if (ERROR_BROKER_WRITE_VERSION_INVALID == ec) {
        _hasVersionError = true;
        AUTIL_LOG(ERROR,
                  "[%s %d] write version invalid, response[%s]",
                  _config.topicName.c_str(),
                  _partitionId,
                  response->ShortDebugString().c_str());
        return;
    }
    if (!response->has_acceptedmsgcount()) {
        AUTIL_LOG(WARN,
                  "[%s %d] Invalid response, miss field[acceptedmsgcount], ec [%s].",
                  _config.topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        if (ec != ERROR_CLIENT_ARPC_ERROR) {
            setLastErrorCode(ERROR_CLIENT_INVALID_RESPONSE);
        }
        return;
    }

    // other fatal error
    if (ec != ERROR_NONE && ec != ERROR_BROKER_BUSY) {
        AUTIL_LOG(WARN,
                  "[%s %u] sendMessage failed for [%s]!",
                  _config.topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        _writeBuffer.resetUnsendCursor();
        return;
    }

    // normal situation
    setLastRefreshTime(_transportAdapter->getLastTransportClosureDoneTime());
    if (ec == ERROR_BROKER_BUSY) {
        AUTIL_LOG(WARN, "[%s %u] broker busy", _config.topicName.c_str(), _partitionId);
    }
    if (_sessionId == -1 && response->has_sessionid()) {
        _sessionId = response->sessionid();
    }
    int64_t acceptedMsgCount = response->acceptedmsgcount();
    int64_t checkpointId = -1;
    if (!_config.safeMode || !response->has_sessionid() || !response->has_committedid() ||
        !response->has_acceptedmsgbeginid()) {
        _writeBuffer.updateBuffer(acceptedMsgCount, checkpointId);
    } else {
        vector<int64_t> msgTimestamps;
        for (int i = 0; i < response->timestamps_size(); ++i) {
            msgTimestamps.emplace_back(response->timestamps(i));
        }
        vector<pair<int64_t, int64_t>> committedCpTs;
        _writeBuffer.updateBuffer(acceptedMsgCount,
                                  response->committedid(),
                                  response->acceptedmsgbeginid(),
                                  checkpointId,
                                  msgTimestamps,
                                  committedCpTs);
        if (_committedFunc && committedCpTs.size() > 0) {
            if (_metricsReporter) {
                _metricsReporter->reportCommitCallbackQps(&_metricsTags);
            }
            _committedFunc(committedCpTs);
        }
    }
    if (checkpointId != -1) {
        setCheckpointId(checkpointId);
    }
}

void SingleSwiftWriter::setTopicChangeCallBack(const std::function<ErrorCode(int64_t)> &func) {
    _topicChangeFunc = func;
}

void SingleSwiftWriter::setCommittedCallBack(const std::function<void(const vector<pair<int64_t, int64_t>> &)> &func) {
    _committedFunc = func;
}

void SingleSwiftWriter::setErrorCallBack(const std::function<void(const ErrorCode &)> &func) { _errorFunc = func; }

void SingleSwiftWriter::setCheckpointId(int64_t checkpointId) {
    ScopedLock lock(_mutex);
    _checkpointId = checkpointId;
}

void SingleSwiftWriter::setLastErrorCode(ErrorCode ec) {
    ScopedLock lock(_mutex);
    _lastErrorCode = ec;
}

ErrorCode SingleSwiftWriter::getLastErrorCode() const {
    ScopedLock lock(_mutex);
    return _lastErrorCode;
}

void SingleSwiftWriter::setLastRefreshTime(int64_t refreshTime) {
    ScopedLock lock(_mutex);
    _refreshTime = refreshTime;
}

int64_t SingleSwiftWriter::getLastRefreshTime() const {
    ScopedLock lock(_mutex);
    return _refreshTime;
}

void SingleSwiftWriter::updateRangeUtil(const RangeUtilPtr rangeUtil) { _writeBuffer.updateRangeUtil(rangeUtil); }

bool SingleSwiftWriter::stealUnsendMessage(std::vector<common::MessageInfo> &msgInfoVec) {
    return _writeBuffer.stealUnsendMessage(msgInfoVec);
}

size_t SingleSwiftWriter::getOneRequestSendBytes() {
    if (getLastErrorCode() != ERROR_NONE) {
        int64_t adjustByteCount = _config.oneRequestSendByteCount / 100;
        AUTIL_LOG(INFO,
                  "adjust send request byte limit from [%ld] to [%ld]",
                  _config.oneRequestSendByteCount,
                  adjustByteCount);
        return adjustByteCount;
    } else {
        return _config.oneRequestSendByteCount;
    }
}
} // namespace client
} // namespace swift
