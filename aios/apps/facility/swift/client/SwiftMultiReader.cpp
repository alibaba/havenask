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
#include "swift/client/SwiftMultiReader.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "autil/CommonMacros.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace swift::protocol;
using namespace autil;
using namespace std;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftMultiReader);

const int64_t MAX_TIME_STAMP = numeric_limits<int64_t>::max();
const int64_t INVALID_TIMESTAMP_VALUE = -2;

SwiftMultiReader::SwiftMultiReader()
    : _readerPos(0), _timestampLimit(0), _readerOrder(RO_DEFAULT), _notifier(new Notifier) {}

SwiftMultiReader::~SwiftMultiReader() {
    for (auto reader : _readerAdapterVec) {
        DELETE_AND_SET_NULL(reader);
    }
    DELETE_AND_SET_NULL(_notifier);
}

bool SwiftMultiReader::addReader(SwiftReader *reader) {
    if (reader) {
        ScopedWriteLock lock(_readerLock);
        SwiftReaderAdapter *readerAdapter = dynamic_cast<SwiftReaderAdapter *>(reader);
        if (NULL == readerAdapter) {
            AUTIL_LOG(ERROR, "add reader[%p] fail, dynamic_cast<swiftReaderAdapter*> fail", reader);
            return false;
        }
        _readerAdapterVec.push_back(readerAdapter);
        _nextTimestampVec.push_back(INVALID_TIMESTAMP_VALUE);
        _checkpointTimestampVec.push_back(INVALID_TIMESTAMP_VALUE);
        _exceedTimestampLimitVec.push_back(false);
        AUTIL_LOG(INFO, "add reader[%p] success", readerAdapter);
        return true;
    }
    AUTIL_LOG(ERROR, "add reader fail, empty pointer");
    return false;
}

ErrorCode SwiftMultiReader::init(const SwiftReaderConfig &config) {
    AUTIL_LOG(ERROR,
              "SwiftMultiReader init should not be called, zk[%s], topic[%s]",
              config.zkPath.c_str(),
              config.topicName.c_str());
    assert(false);
    return ERROR_CLIENT_INIT_FAILED;
}

ErrorCode SwiftMultiReader::read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout) {
    ScopedWriteLock lock(_readerLock);
    return readMsg(timeStamp, msgs, timeout);
}

ErrorCode SwiftMultiReader::read(int64_t &timeStamp, protocol::Message &msg, int64_t timeout) {
    ScopedWriteLock lock(_readerLock);
    return readMsg(timeStamp, msg, timeout);
}

int64_t SwiftMultiReader::getCheckpointTimestamp() const {
    int64_t minTime = MAX_TIME_STAMP;
    for (size_t idx = 0; idx < _checkpointTimestampVec.size(); ++idx) {
        if (minTime > _checkpointTimestampVec[idx]) {
            minTime = _checkpointTimestampVec[idx];
        }
    }
    return std::max(minTime, (int64_t)0);
}

void SwiftMultiReader::getReaderPos() {
    if (RO_SEQ == _readerOrder) {
        for (size_t idx = 0; idx < _nextTimestampVec.size(); ++idx) {
            _readerPos = (_readerPos + 1) % _nextTimestampVec.size();
            if (!_exceedTimestampLimitVec[_readerPos]) {
                break;
            }
        }
    } else {
        int64_t minTime = MAX_TIME_STAMP;
        for (size_t idx = 0; idx < _nextTimestampVec.size(); ++idx) {
            if (_exceedTimestampLimitVec[idx]) {
                continue;
            }
            if (minTime > _nextTimestampVec[idx]) {
                minTime = _nextTimestampVec[idx];
                _readerPos = idx;
            }
        }
    }
}

ErrorCode SwiftMultiReader::checkTimestampLimit(ErrorCode ec) {
    if (ec != ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT) {
        return ec;
    }
    ErrorCode retEc = ec;
    for (auto ts : _exceedTimestampLimitVec) {
        if (!ts) {
            retEc = ERROR_CLIENT_NO_MORE_MESSAGE;
            break;
        }
    }
    return retEc;
}

void SwiftMultiReader::checkCurrentError(protocol::ErrorCode &errorCode, string &errorMsg) const {
    ScopedReadLock lock(_readerLock);
    errorCode = ERROR_NONE;
    for (auto reader : _readerAdapterVec) {
        string error;
        protocol::ErrorCode ec = ERROR_NONE;
        reader->checkCurrentError(ec, error);
        if (ERROR_NONE != ec) {
            errorCode = ec;
            errorMsg += (error + ";");
        }
    }
}

SwiftPartitionStatus SwiftMultiReader::getSwiftPartitionStatus() {
    ScopedReadLock lock(_readerLock);
    SwiftPartitionStatus rePartStatus;
    rePartStatus.refreshTime = MAX_TIME_STAMP;
    rePartStatus.maxMessageId = -1;
    rePartStatus.maxMessageTimestamp = -1;
    for (const auto reader : _readerAdapterVec) {
        SwiftPartitionStatus partStatus;
        partStatus = reader->getSwiftPartitionStatus();
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

void SwiftMultiReader::setRequiredFieldNames(const vector<string> &fieldNames) {
    ScopedWriteLock lock(_readerLock);
    for (auto reader : _readerAdapterVec) {
        reader->setRequiredFieldNames(fieldNames);
    }
}

void SwiftMultiReader::setFieldFilterDesc(const string &fieldFilterDesc) {
    ScopedWriteLock lock(_readerLock);
    for (auto reader : _readerAdapterVec) {
        reader->setFieldFilterDesc(fieldFilterDesc);
    }
}

void SwiftMultiReader::setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) {
    ScopedWriteLock lock(_readerLock);
    int64_t maxAcceptTimestamp = timestampLimit;
    for (auto reader : _readerAdapterVec) {
        int64_t lastMsgTime = reader->getMaxLastMsgTimestamp();
        maxAcceptTimestamp = std::max(maxAcceptTimestamp, lastMsgTime);
    }

    int64_t tmpAcTimestamp = -1;
    for (size_t idx = 0; idx < _readerAdapterVec.size(); ++idx) {
        _readerAdapterVec[idx]->setTimestampLimit(maxAcceptTimestamp, tmpAcTimestamp);
        assert(tmpAcTimestamp == maxAcceptTimestamp);
        if (_nextTimestampVec[idx] < maxAcceptTimestamp) {
            _exceedTimestampLimitVec[idx] = false;
        }
    }
    _timestampLimit = maxAcceptTimestamp;
    acceptTimestamp = _timestampLimit;
}

vector<string> SwiftMultiReader::getRequiredFieldNames() {
    ScopedReadLock lock(_readerLock);
    assert(_readerAdapterVec.size() != 0);
    return _readerAdapterVec[_readerPos]->getRequiredFieldNames();
}

bool SwiftMultiReader::updateCommittedCheckpoint(int64_t checkpoint) {
    bool ret = true;
    for (auto reader : _readerAdapterVec) {
        if (!reader->updateCommittedCheckpoint(checkpoint)) {
            ret = false;
        }
    }
    return ret;
}

ErrorCode SwiftMultiReader::seekByTimestamp(int64_t timeStamp, bool force) {
    ScopedWriteLock lock(_readerLock);
    ErrorCode ecAll = ERROR_NONE;
    for (size_t idx = 0; idx < _readerAdapterVec.size(); ++idx) {
        ErrorCode ec = _readerAdapterVec[idx]->seekByTimestamp(timeStamp, force);
        if (ERROR_NONE == ec) {
            _nextTimestampVec[idx] = _readerAdapterVec[idx]->getNextMsgTimestamp();
            _checkpointTimestampVec[idx] = _readerAdapterVec[idx]->getCheckpointTimestamp();
            _exceedTimestampLimitVec[idx] = false;
        } else {
            ecAll = ec;
        }
    }
    return ecAll;
}

ErrorCode SwiftMultiReader::seekByProgress(const ReaderProgress &progress, bool force) {
    if (progress.topicprogress_size() == 0) {
        AUTIL_LOG(ERROR, "empty proress");
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }
    ScopedWriteLock lock(_readerLock);
    ErrorCode ecAll = ERROR_NONE;
    for (size_t idx = 0; idx < _readerAdapterVec.size(); ++idx) {
        auto config = _readerAdapterVec[idx]->getReaderConfig();
        ReaderProgress readerProgress;
        if (!filterReaderProgress(progress, config, readerProgress)) {
            AUTIL_LOG(ERROR, "not found topic [%s] progress", config.topicName.c_str());
            return ERROR_CLIENT_INVALID_PARAMETERS;
        }
        ErrorCode ec = _readerAdapterVec[idx]->seekByProgress(readerProgress, force);
        if (ERROR_NONE == ec) {
            _nextTimestampVec[idx] = INVALID_TIMESTAMP_VALUE; // seek will clear buffer
            _checkpointTimestampVec[idx] = INVALID_TIMESTAMP_VALUE;
            _exceedTimestampLimitVec[idx] = false;
        } else {
            ecAll = ec;
        }
    }
    return ecAll;
}

bool SwiftMultiReader::filterReaderProgress(const protocol::ReaderProgress &progress,
                                            const SwiftReaderConfig &config,
                                            protocol::ReaderProgress &outProgress) {
    for (size_t i = 0; i < progress.topicprogress_size(); i++) {
        const TopicReaderProgress &topicProgress = progress.topicprogress(i);
        if (topicProgress.topicname() == config.topicName &&
            topicProgress.uint8filtermask() == config.swiftFilter.uint8filtermask() &&
            topicProgress.uint8maskresult() == config.swiftFilter.uint8maskresult()) {
            *outProgress.add_topicprogress() = topicProgress;
            return true;
        }
    }
    return false;
}

ErrorCode SwiftMultiReader::seekByMessageId(int64_t msgId) {
    ScopedWriteLock lock(_readerLock);
    ErrorCode ecAll = ERROR_NONE;
    for (size_t idx = 0; idx < _readerAdapterVec.size(); ++idx) {
        ErrorCode ec = _readerAdapterVec[idx]->seekByMessageId(msgId);
        if (ERROR_NONE == ec) {
            _nextTimestampVec[idx] = INVALID_TIMESTAMP_VALUE; // seek will clear buffer
            _checkpointTimestampVec[idx] = INVALID_TIMESTAMP_VALUE;
            _exceedTimestampLimitVec[idx] = false;
        } else {
            ecAll = ec;
        }
    }
    return ecAll;
}

common::SchemaReader *SwiftMultiReader::getSchemaReader(const char *data, ErrorCode &ec) {
    ScopedWriteLock lock(_readerLock);
    return _readerAdapterVec[_readerPos]->getSchemaReader(data, ec);
}

ErrorCode SwiftMultiReader::getReaderProgress(ReaderProgress &progress) const {
    ErrorCode retEc = ERROR_NONE;
    ScopedReadLock lock(_readerLock);
    for (size_t idx = 0; idx < _readerAdapterVec.size(); ++idx) {
        ErrorCode ec = _readerAdapterVec[idx]->getReaderProgress(progress);
        retEc = (ec == ERROR_NONE) ? retEc : ec;
    }
    return retEc;
}

bool SwiftMultiReader::setReaderOrder(const std::string &order) {
    if ("sequence" == order) {
        _readerOrder = RO_SEQ;
    } else if ("default" == order) {
        _readerOrder = RO_DEFAULT;
    } else {
        return false;
    }
    return true;
}

} // namespace client
} // namespace swift
