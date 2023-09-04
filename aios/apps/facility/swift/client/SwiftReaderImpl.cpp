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
#include "swift/client/SwiftReaderImpl.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "swift/client/Notifier.h"
#include "swift/client/RangeUtil.h"
#include "swift/client/SwiftSinglePartitionReader.h"
#include "swift/common/ProgressUtil.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace common {
class SchemaReader;
} // namespace common
} // namespace swift

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::network;
namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftReaderImpl);

static const uint64_t MIN_BUFFER_SIZE = 1;
static const int64_t MAX_TIME_STAMP = numeric_limits<int64_t>::max();

SwiftReaderImpl::SwiftReaderImpl(const SwiftAdminAdapterPtr &adminAdapter,
                                 const SwiftRpcChannelManagerPtr &channelManager,
                                 const TopicInfo &topicInfo,
                                 Notifier *notifier,
                                 string logicTopicName,
                                 monitor::ClientMetricsReporter *reporter)
    : _adminAdapter(adminAdapter)
    , _logicTopicName(logicTopicName)
    , _channelManager(channelManager)
    , _timeLimitMode(false)
    , _checkpointTimestamp(-1)
    , _notifier(notifier)
    , _needFillAll(true)
    , _lastReadIndex(-1)
    , _bufferCursor(0)
    , _bufferReadIndex(-1)
    , _isBufferRead(false)
    , _isTopicChanged(false)
    , _partitionCount(topicInfo.partitioncount())
    , _topicVersion(topicInfo.has_modifytime() ? topicInfo.modifytime() : -1)
    , _isTopicLongPollingEnabled(topicInfo.has_enablelongpolling() ? topicInfo.enablelongpolling() : false)
    , _reporter(reporter) {
    if (NULL == notifier) {
        _notifier = new Notifier;
        _ownNotifier = true;
    } else {
        _notifier = notifier;
        _ownNotifier = false;
    }
}

SwiftReaderImpl::~SwiftReaderImpl() {
    clearReaders();
    if (_ownNotifier) {
        DELETE_AND_SET_NULL(_notifier);
    }
}

void SwiftReaderImpl::clearReaders() {
    for (vector<SwiftSinglePartitionReader *>::iterator it = _readers.begin(); it != _readers.end(); ++it) {
        delete (*it);
    }
    _readers.clear();
    _exceedLimitVec.clear();
    _timestamps.clear();
    _messageBuffer.clear_msgs();
    _bufferCursor = 0;
    _bufferReadIndex = -1;
}

ErrorCode SwiftReaderImpl::init(const SwiftReaderConfig &config) {
    AUTIL_LOG(INFO, "init topic[%s]'s readerImpl", config.topicName.c_str());
    _config = config;
    if (_logicTopicName.empty()) {
        _logicTopicName = _config.topicName;
    }
    if (_config.readPartVec.size() == 0) {
        RangeUtil rangeUtil(_partitionCount);
        _config.readPartVec = rangeUtil.getPartitionIds(_config.swiftFilter.from(), _config.swiftFilter.to());
    }
    if (_config.readPartVec.size() == 0) {
        return ERROR_CLIENT_INVALID_PARTITIONID;
    }

    for (size_t i = 0; i < _config.readPartVec.size(); ++i) {
        if (_config.readPartVec[i] >= _partitionCount) {
            AUTIL_LOG(ERROR,
                      "topic[%s] partition id[%u] is greater than partition"
                      " count[%u]",
                      _config.topicName.c_str(),
                      _config.readPartVec[i],
                      _partitionCount);
            return ERROR_CLIENT_INVALID_PARTITIONID;
        }
    }

    initReaders(_config.readPartVec, _partitionCount);
    if (_config.requiredFields.size() != 0) {
        setRequiredFieldNames(_config.requiredFields);
    }
    if (!_config.fieldFilterDesc.empty()) {
        setFieldFilterDesc(_config.fieldFilterDesc);
    }
    return ERROR_NONE;
}

void SwiftReaderImpl::initReaders(const vector<uint32_t> &readPartVec, int32_t partCount) {
    SwiftReaderConfig modifiedConfig = _config;
    modifiedConfig.readBufferSize = modifiedConfig.readBufferSize / readPartVec.size();
    if (modifiedConfig.readBufferSize < MIN_BUFFER_SIZE) {
        modifiedConfig.readBufferSize = MIN_BUFFER_SIZE;
    }
    RangeUtil rangeUtil(partCount);
    for (vector<uint32_t>::const_iterator it = readPartVec.begin(); it != readPartVec.end(); ++it) {
        // don't modify config filter for single reader, swift part count may change, otherwise cann't read old message
        Filter filter = _config.swiftFilter;
        uint32_t from = 0, to = 0;
        if (rangeUtil.getPartRange(*it, from, to)) {
            if (from > _config.swiftFilter.from()) {
                filter.set_from(from);
            }
            if (to < _config.swiftFilter.to()) {
                filter.set_to(to);
            }
        }
        SwiftSinglePartitionReader *reader = new SwiftSinglePartitionReader(
            _adminAdapter, _channelManager, *it, modifiedConfig, _topicVersion, _notifier, _reporter);
        reader->setFilterRange(filter.from(), filter.to());
        reader->setDecompressThreadPool(_decompressThreadPool);
        reader->setTopicLongPollingEnabled(_isTopicLongPollingEnabled);
        _readers.push_back(reader);
        _exceedLimitVec.push_back(false);
        _timestamps.push_back(-1);
        reader->setCallBackFunc(std::bind(&SwiftReaderImpl::setTopicChanged, this, std::placeholders::_1));
    }
}

ErrorCode SwiftReaderImpl::read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout) {
    ScopedLock lock(_mutex);
    if (_bufferCursor < _messageBuffer.msgs_size()) {
        msgs.clear_msgs();
        for (; _bufferCursor < _messageBuffer.msgs_size(); ++_bufferCursor) {
            msgs.add_msgs()->Swap(_messageBuffer.mutable_msgs(_bufferCursor));
        }
        timeStamp = _checkpointTimestamp;
        return ERROR_NONE;
    }
    return doBatchRead(timeStamp, msgs, timeout);
}

ErrorCode SwiftReaderImpl::doBatchRead(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout) {
    ErrorCode rec = reportFatalError();
    if (rec != ERROR_NONE) {
        timeStamp = _checkpointTimestamp;
        return rec;
    }
    ErrorCode checkLimit = checkTimestampLimit();
    if (checkLimit != ERROR_NONE) {
        timeStamp = _checkpointTimestamp;
        return checkLimit;
    }
    if (tryRead(msgs)) {
        tryFillBuffer();
        timeStamp = _checkpointTimestamp;
        return ERROR_NONE;
    }
    rec = fillBuffer(timeout);
    checkLimit = checkTimestampLimit();
    if (checkLimit != ERROR_NONE) {
        timeStamp = _checkpointTimestamp;
        return checkLimit;
    }
    if (tryRead(msgs)) {
        timeStamp = _checkpointTimestamp;
        return ERROR_NONE;
    }
    timeStamp = _checkpointTimestamp;
    return sealedTopicReadFinish(rec);
}

ErrorCode SwiftReaderImpl::read(int64_t &timeStamp, Message &msg, int64_t timeout) {
    ScopedLock lock(_mutex);
    if (_messageBuffer.msgs_size() <= _bufferCursor) {
        _messageBuffer.clear_msgs();
        _bufferCursor = 0;
        _isBufferRead = true;
        ErrorCode ec = doBatchRead(timeStamp, _messageBuffer, timeout);
        _isBufferRead = false;
        if (ec != ERROR_NONE) {
            return ec;
        }
    }
    if (_messageBuffer.msgs_size() <= _bufferCursor) {
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    _messageBuffer.mutable_msgs(_bufferCursor)->Swap(&msg);
    ++_bufferCursor;
    if (_bufferCursor >= _messageBuffer.msgs_size()) {
        timeStamp = _checkpointTimestamp;
    } else {
        timeStamp = min(_checkpointTimestamp, _messageBuffer.msgs(_bufferCursor).timestamp());
    }
    return ERROR_NONE;
}

ErrorCode SwiftReaderImpl::seekByTimestamp(int64_t timestamp, bool force) {
    ScopedLock lock(_mutex);
    if (timestamp < 0 || MAX_TIME_STAMP == timestamp) {
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }
    int64_t currTime = TimeUtility::currentTime();
    if (timestamp > currTime) {
        AUTIL_LOG(WARN,
                  "seek time[%ld] larger current time[%ld], "
                  "use current time to seek",
                  timestamp,
                  currTime);
        timestamp = currTime;
    }
    ErrorCode rec = ERROR_NONE;
    int64_t lastTimeStamp = -1;
    for (size_t i = 0; i < _readers.size(); ++i) {
        if (_bufferReadIndex == i) {
            if (force) {
                _messageBuffer.clear_msgs();
                _bufferCursor = 0;
            }
            while (_bufferCursor < _messageBuffer.msgs_size()) {
                if (_messageBuffer.msgs(_bufferCursor).timestamp() < timestamp) {
                    _bufferCursor++;
                } else {
                    break;
                }
            }
            if (_bufferCursor < _messageBuffer.msgs_size()) {
                lastTimeStamp = _messageBuffer.msgs(_bufferCursor).timestamp();
            } else {
                lastTimeStamp = _readers[i]->getNextMsgTimestamp();
            }
        } else {
            lastTimeStamp = _readers[i]->getNextMsgTimestamp();
        }
        if (force || lastTimeStamp < timestamp) {
            ErrorCode ec = ERROR_NONE;
            ec = _readers[i]->seekByTimestamp(timestamp);
            if (ec != ERROR_NONE) {
                rec = ec;
            }
        }
    }
    if (rec == ERROR_NONE) {
        _checkpointTimestamp = getMinCheckpointTimestamp();
        AUTIL_LOG(INFO,
                  "[%s] seek to[%ld] success, checkpoint timestamp[%ld]",
                  _config.topicName.c_str(),
                  timestamp,
                  _checkpointTimestamp);
        timestamp = currTime;
    } else {
        AUTIL_LOG(INFO,
                  "[%s] seek to[%ld] fail, error[%s]",
                  _config.topicName.c_str(),
                  timestamp,
                  ErrorCode_Name(rec).c_str());
    }
    return rec;
}

ErrorCode SwiftReaderImpl::seekByMessageId(int64_t msgId) {
    ScopedLock lock(_mutex);
    ErrorCode rec = ERROR_NONE;
    _messageBuffer.clear_msgs();
    _bufferCursor = 0;
    for (vector<SwiftSinglePartitionReader *>::iterator it = _readers.begin(); it != _readers.end(); ++it) {
        ErrorCode ec = ERROR_NONE;
        ec = (*it)->seekByMessageId(msgId);
        if (ec != ERROR_NONE) {
            rec = ec;
        }
    }
    if (_timeLimitMode) {
        for (size_t i = 0; i < _exceedLimitVec.size(); i++) {
            _exceedLimitVec[i] = false;
        }
    }

    return rec;
}

ErrorCode SwiftReaderImpl::seekByProgress(const ReaderProgress &progress, bool force) {
    if (progress.topicprogress_size() != 1) {
        AUTIL_LOG(WARN, "reader progress size not equal 1");
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }
    common::ProgressUtil progressUtil;
    if (!progressUtil.updateProgress(progress.topicprogress(0))) {
        AUTIL_LOG(ERROR, "update reader progress failed, [%s]", progress.ShortDebugString().c_str());
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }
    // check range
    int64_t timestamp = 0;
    int64_t lastTimeStamp = -1;
    ErrorCode retEc = ERROR_NONE;
    ScopedLock lock(_mutex);
    for (size_t i = 0; i < _readers.size(); ++i) {
        auto partRange = _readers[i]->getFilterRange();
        timestamp = progressUtil.getCoverMinTimestamp(partRange.first, partRange.second);
        if (_bufferReadIndex == i) {
            if (force) {
                _messageBuffer.clear_msgs();
                _bufferCursor = 0;
            }
            while (_bufferCursor < _messageBuffer.msgs_size()) {
                if (_messageBuffer.msgs(_bufferCursor).timestamp() < timestamp) {
                    _bufferCursor++;
                } else {
                    break;
                }
            }
            if (_bufferCursor < _messageBuffer.msgs_size()) {
                lastTimeStamp = _messageBuffer.msgs(_bufferCursor).timestamp();
            } else {
                lastTimeStamp = _readers[i]->getNextMsgTimestamp();
            }
        } else {
            lastTimeStamp = _readers[i]->getNextMsgTimestamp();
        }
        if (force || lastTimeStamp < timestamp) {
            ErrorCode ec = _readers[i]->seekByTimestamp(timestamp);
            retEc = (ec == ERROR_NONE) ? retEc : ec;
        }
        AUTIL_LOG(INFO,
                  "reader[%ld] cover timestamp[%ld], topic[%s], u8filter[%d], u8mask[%d], "
                  " cover from[%d], cover to[%d]",
                  i,
                  timestamp,
                  _logicTopicName.c_str(),
                  _config.swiftFilter.uint8filtermask(),
                  _config.swiftFilter.uint8maskresult(),
                  partRange.first,
                  partRange.second);
    }
    if (retEc == ERROR_NONE) {
        _checkpointTimestamp = getMinCheckpointTimestamp();
        AUTIL_LOG(
            INFO, "[%s] seek success, min checkpoint timestamp[%ld]", _logicTopicName.c_str(), _checkpointTimestamp);
    } else {
        AUTIL_LOG(INFO, "[%s] seek fail, error[%s]", _logicTopicName.c_str(), ErrorCode_Name(retEc).c_str());
    }
    return retEc;
}

void SwiftReaderImpl::checkCurrentError(ErrorCode &errorCode, string &errorMsg) const {
    ScopedLock lock(_mutex);
    errorCode = ERROR_NONE;
    for (vector<SwiftSinglePartitionReader *>::const_iterator it = _readers.begin(); it != _readers.end(); ++it) {
        ErrorCode ec = ERROR_NONE;
        string msg;
        (*it)->checkCurrentError(ec, msg);
        if (ec != ERROR_NONE) {
            errorCode = ec;
            errorMsg += (msg + ";");
        }
    }
}

SwiftPartitionStatus SwiftReaderImpl::getSwiftPartitionStatus() {
    SwiftPartitionStatus rePartStatus;
    rePartStatus.refreshTime = MAX_TIME_STAMP;
    rePartStatus.maxMessageId = -1;
    rePartStatus.maxMessageTimestamp = -1;

    ScopedLock lock(_statusMutex);
    uint64_t curTime = TimeUtility::currentTime();
    for (vector<SwiftSinglePartitionReader *>::const_iterator it = _readers.begin(); it != _readers.end(); ++it) {
        SwiftPartitionStatus partStatus;
        partStatus = (*it)->getSwiftPartitionStatus(curTime);
        if (partStatus.maxMessageTimestamp > rePartStatus.maxMessageTimestamp) {
            rePartStatus.maxMessageId = partStatus.maxMessageId;
            rePartStatus.maxMessageTimestamp = partStatus.maxMessageTimestamp;
        }

        if (partStatus.refreshTime < rePartStatus.refreshTime) {
            rePartStatus.refreshTime = partStatus.refreshTime;
        }
    }
    if (rePartStatus.refreshTime == MAX_TIME_STAMP) {
        rePartStatus.refreshTime = -1;
    }
    return rePartStatus;
}

ErrorCode SwiftReaderImpl::reportFatalError() {
    ErrorCode ec = ERROR_NONE;
    bool allFinished = true;
    for (vector<SwiftSinglePartitionReader *>::iterator it = _readers.begin(); it != _readers.end(); ++it) {
        ec = (*it)->reportFatalError(true);
        if (ERROR_NONE == ec) {
            allFinished = false;
        } else if (ERROR_SEALED_TOPIC_READ_FINISH == ec) {
            allFinished &= true;
        } else { // other error, reset same error
            for (++it; it != _readers.end(); ++it) {
                if ((*it)->reportFatalError(false) == ec) {
                    (*it)->reportFatalError(true);
                }
            }
            return ec;
        }
    }
    return allFinished ? ERROR_SEALED_TOPIC_READ_FINISH : ERROR_NONE;
}

ErrorCode SwiftReaderImpl::sealedTopicReadFinish(ErrorCode ec) {
    if (ec != ERROR_CLIENT_NO_MORE_MESSAGE) {
        return ec;
    }
    bool allFinished = true;
    for (vector<SwiftSinglePartitionReader *>::iterator it = _readers.begin(); it != _readers.end(); ++it) {
        allFinished &= (*it)->sealedTopicReadFinish();
    }
    return allFinished ? ERROR_SEALED_TOPIC_READ_FINISH : ec;
}

int64_t SwiftReaderImpl::getUnReadMsgCount() const {
    int64_t unReadMsgCount = 0;
    for (vector<SwiftSinglePartitionReader *>::const_iterator it = _readers.begin(); it != _readers.end(); ++it) {
        unReadMsgCount += (*it)->getUnReadMsgCount();
    }
    return unReadMsgCount;
}

bool SwiftReaderImpl::hasUnReadMsg() const {
    vector<SwiftSinglePartitionReader *>::const_iterator it = _readers.begin();
    for (; it != _readers.end(); ++it) {
        if ((*it)->hasUnReadMsg()) {
            return true;
        }
    }
    return false;
}

bool SwiftReaderImpl::tryRead(protocol::Messages &msgs) {
    _needFillAll = false;
    _lastReadIndex = -1;
    msgs.clear_msgs();
    int64_t firstMsgTimestamp = MAX_TIME_STAMP;
    size_t readerIndex = 0;
    size_t size = _readers.size();
    for (size_t i = 0; i < size; ++i) {
        if (_timeLimitMode && _exceedLimitVec[i]) {
            continue;
        }
        int64_t f = _readers[i]->getFirstMsgTimestamp();
        if (-1 == f) {
            _needFillAll = true;
            continue;
        }
        if (f < firstMsgTimestamp) {
            firstMsgTimestamp = f;
            readerIndex = i;
        }
    }
    bool ret = firstMsgTimestamp != MAX_TIME_STAMP && _readers[readerIndex]->tryRead(msgs);
    _checkpointTimestamp = getMinCheckpointTimestamp();
    _lastReadIndex = readerIndex;
    if (_isBufferRead) {
        _bufferReadIndex = readerIndex;
    }
    return ret;
}

int64_t SwiftReaderImpl::tryFillBuffer(int64_t currentTime) {
    if (!_needFillAll) {
        return _readers[_lastReadIndex]->tryFillBuffer(currentTime);
    }
    int64_t rt = MAX_TIME_STAMP;
    for (size_t i = 0; i < _readers.size(); ++i) {
        if (_timeLimitMode && _exceedLimitVec[i]) {
            continue;
        }
        int64_t interval = _readers[i]->tryFillBuffer(currentTime);
        if (interval < rt) {
            rt = interval;
        }
    }
    return rt;
}

ErrorCode SwiftReaderImpl::fillBuffer(int64_t timeout) {
    if (timeout <= 0) {
        _needFillAll = true;
        tryFillBuffer();
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    int64_t leftTime = timeout;
    int64_t endTime = TimeUtility::currentTime() + leftTime;
    do {
        if (unlikely(isTopicChanged())) {
            AUTIL_LOG(INFO, "[%p] topic changed name [%s]", this, _config.topicName.c_str());
            return ERROR_CLIENT_NO_MORE_MESSAGE;
        }
        _notifier->setNeedNotify(true);
        int64_t currentTime = TimeUtility::currentTime();
        _needFillAll = true;
        int64_t interval = tryFillBuffer(currentTime);
        if (hasUnReadMsg()) {
            _notifier->setNeedNotify(false);
            return ERROR_NONE;
        }
        int64_t now = TimeUtility::currentTime();
        leftTime = endTime - now;
        if (leftTime <= 0) {
            break;
        }

        interval -= now - currentTime;
        if (interval > 0) {
            interval = min(interval, leftTime);
            _notifier->wait(interval);
        }
    } while (true);
    _notifier->setNeedNotify(false);
    return ERROR_CLIENT_NO_MORE_MESSAGE;
}

int64_t SwiftReaderImpl::getFirstMsgTimestamp() const {
    int64_t firstMsgTimestamp = MAX_TIME_STAMP;
    for (size_t i = 0; i < _readers.size(); ++i) {
        int64_t re = _readers[i]->getFirstMsgTimestamp();
        if (-1 == re) {
            continue;
        }
        if (re < firstMsgTimestamp) {
            firstMsgTimestamp = re;
        }
    }
    if (MAX_TIME_STAMP == firstMsgTimestamp) {
        return -1;
    }
    return firstMsgTimestamp;
}

int64_t SwiftReaderImpl::getNextMsgTimestamp() const {
    int64_t nextMsgTimestamp = MAX_TIME_STAMP;
    size_t size = _readers.size();
    for (size_t i = 0; i < size; ++i) {
        int64_t re = _readers[i]->getNextMsgTimestamp();
        if (re < nextMsgTimestamp) {
            nextMsgTimestamp = re;
        }
    }
    if (_bufferCursor < _messageBuffer.msgs_size()) {
        nextMsgTimestamp = min(nextMsgTimestamp, _messageBuffer.msgs(_bufferCursor).timestamp());
    }
    return nextMsgTimestamp;
}

int64_t SwiftReaderImpl::getCheckpointTimestamp() const {
    int64_t checkpointTimestamp = _checkpointTimestamp;
    if (_bufferCursor < _messageBuffer.msgs_size()) {
        checkpointTimestamp = min(checkpointTimestamp, _messageBuffer.msgs(_bufferCursor).timestamp());
    }
    return checkpointTimestamp;
}

int64_t SwiftReaderImpl::getMinCheckpointTimestamp() const {
    if (_readers.size() == 0) {
        return -1;
    }
    int64_t checkpointTimestamp = MAX_TIME_STAMP;
    size_t size = _readers.size();
    for (size_t i = 0; i < size; ++i) {
        int64_t re = _readers[i]->getCheckpointTimestamp().first;
        if (re < checkpointTimestamp) {
            checkpointTimestamp = re;
        }
    }
    return checkpointTimestamp;
}

void SwiftReaderImpl::setRequiredFieldNames(const vector<string> &fieldNames) {
    ScopedLock lock(_mutex);
    _fieldNames = fieldNames;
    for (size_t i = 0; i < _readers.size(); i++) {
        _readers[i]->setRequiredFieldNames(fieldNames);
        if (_bufferReadIndex == i && _bufferCursor < _messageBuffer.msgs_size()) {
            int64_t msgId = _messageBuffer.msgs(_bufferCursor).msgid();
            _readers[i]->seekByMessageId(msgId);
        }
    }
    _messageBuffer.clear_msgs();
    _bufferCursor = 0;
}

std::vector<std::string> SwiftReaderImpl::getRequiredFieldNames() {
    ScopedLock lock(_mutex);
    return _fieldNames;
}

bool SwiftReaderImpl::updateCommittedCheckpoint(int64_t checkpoint) {
    bool isSuc = true;
    for (size_t i = 0; i < _readers.size(); ++i) {
        if (!_readers[i]->updateCommittedCheckpoint(checkpoint)) {
            isSuc = false;
        }
    }
    return isSuc;
}

void SwiftReaderImpl::setFieldFilterDesc(const string &fieldFilterDesc) {
    ScopedLock lock(_mutex);
    _fieldFilterDesc = fieldFilterDesc;
    for (size_t i = 0; i < _readers.size(); ++i) {
        _readers[i]->setFieldFilterDesc(fieldFilterDesc);
        if (_bufferReadIndex == i && _bufferCursor < _messageBuffer.msgs_size()) {
            int64_t msgId = _messageBuffer.msgs(_bufferCursor).msgid();
            _readers[i]->seekByMessageId(msgId);
        }
    }
    _messageBuffer.clear_msgs();
    _bufferCursor = 0;
}

void SwiftReaderImpl::setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) {
    ScopedLock lock(_mutex);
    if (timestampLimit != MAX_TIME_STAMP) {
        _timeLimitMode = true;
        int64_t maxAcceptTimestamp = timestampLimit;
        for (size_t i = 0; i < _readers.size(); i++) {
            int64_t lastMsgTime = _readers[i]->getLastMsgTimestamp();
            maxAcceptTimestamp = std::max(maxAcceptTimestamp, lastMsgTime);
        }
        for (size_t i = 0; i < _readers.size(); i++) {
            _readers[i]->setTimestampLimit(maxAcceptTimestamp);
        }
        for (size_t i = 0; i < _readers.size(); i++) {
            int64_t nextTimestamp = _readers[i]->getNextMsgTimestamp();
            if (nextTimestamp == -1 || nextTimestamp <= maxAcceptTimestamp) {
                _exceedLimitVec[i] = false;
            }
        }
        acceptTimestamp = maxAcceptTimestamp;
    } else {
        _timeLimitMode = false;
        for (size_t i = 0; i < _readers.size(); i++) {
            _readers[i]->setTimestampLimit(MAX_TIME_STAMP);
            _exceedLimitVec[i] = false;
        }
        acceptTimestamp = MAX_TIME_STAMP;
    }
}

ErrorCode SwiftReaderImpl::checkTimestampLimit() {
    if (_timeLimitMode) {
        size_t exceedCount = 0;
        for (size_t i = 0; i < _readers.size(); i++) {
            if (!_exceedLimitVec[i]) {
                _exceedLimitVec[i] = _readers[i]->exceedTimestampLimit();
            }
            if (_exceedLimitVec[i]) {
                exceedCount++;
            }
        }
        if (exceedCount == _readers.size()) {
            return ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT;
        }
    }
    return ERROR_NONE;
}

protocol::ErrorCode SwiftReaderImpl::setTopicChanged(int64_t topicVersion) {
    AUTIL_LOG(INFO, "topic[%s] version[%ld] set change", _config.topicName.c_str(), topicVersion);
    _isTopicChanged = true;
    _topicVersion = topicVersion;
    return ERROR_NONE;
}

int64_t SwiftReaderImpl::getMaxLastMsgTimestamp() const {
    int64_t maxAcceptTimestamp = -1;
    for (size_t i = 0; i < _readers.size(); i++) {
        int64_t lastMsgTime = _readers[i]->getLastMsgTimestamp();
        maxAcceptTimestamp = std::max(maxAcceptTimestamp, lastMsgTime);
    }
    return maxAcceptTimestamp;
}

void SwiftReaderImpl::setTopicLongPollingEnabled(bool enable) {
    AUTIL_LOG(INFO,
              "topic [%s] long polling enable is updated from  %d to %d",
              _logicTopicName.c_str(),
              _isTopicLongPollingEnabled,
              enable);
    if (_isTopicLongPollingEnabled != enable) {
        _isTopicLongPollingEnabled = enable;
        for (auto *reader : _readers) {
            reader->setTopicLongPollingEnabled(enable);
        }
    }
}

common::SchemaReader *SwiftReaderImpl::getSchemaReader(const char *data, ErrorCode &ec) {
    ec = ERROR_ADMIN_TOPIC_NOT_SUPPORT_SCHEMA;
    AUTIL_LOG(WARN, "%s is not a schema topic, ERROR_ADMIN_TOPIC_NOT_SUPPORT_SCHEMA", _logicTopicName.c_str());
    return NULL;
}

ErrorCode SwiftReaderImpl::getReaderProgress(ReaderProgress &progress) const {
    ErrorCode retEc = ERROR_NONE;
    TopicReaderProgress *topicProgress = progress.add_topicprogress();
    topicProgress->set_topicname(_logicTopicName);
    topicProgress->set_uint8filtermask(_config.swiftFilter.uint8filtermask());
    topicProgress->set_uint8maskresult(_config.swiftFilter.uint8maskresult());
    ScopedLock lock(_mutex);
    for (size_t index = 0; index < _readers.size(); ++index) {
        PartReaderProgress *partProgress = topicProgress->add_partprogress();
        auto range = _readers[index]->getFilterRange();
        partProgress->set_from(range.first);
        partProgress->set_to(range.second);
        auto cpInfo = _readers[index]->getCheckpointTimestamp();
        partProgress->set_timestamp(cpInfo.first);
        partProgress->set_offsetinrawmsg(cpInfo.second);
        // adjust timestamp
        if (_bufferReadIndex == index) {
            if (_bufferCursor < _messageBuffer.msgs_size()) {
                const auto &msg = _messageBuffer.msgs(_bufferCursor);
                if (partProgress->timestamp() >= msg.timestamp()) {
                    partProgress->set_timestamp(msg.timestamp());
                    if (msg.offsetinrawmsg() >= common::OFFSET_IN_MERGE_MSG_BASE) {
                        partProgress->set_offsetinrawmsg(msg.offsetinrawmsg() - common::OFFSET_IN_MERGE_MSG_BASE);
                    }
                }
            }
        }
    }
    return retEc;
}

} // namespace client
} // namespace swift
