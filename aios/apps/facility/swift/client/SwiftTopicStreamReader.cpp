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
#include "swift/client/SwiftTopicStreamReader.h"

#include <algorithm>
#include <type_traits>
#include <utility>

#include "autil/Log.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace common {
class SchemaReader;
} // namespace common

namespace client {
class SwiftReaderConfig;

AUTIL_DECLARE_AND_SETUP_LOGGER(swift, SwiftTopicStreamReader);

SwiftTopicStreamReader::SwiftTopicStreamReader() : _currentIdx(-1), _currentReader(nullptr) {}

SwiftTopicStreamReader::~SwiftTopicStreamReader() {}

protocol::ErrorCode SwiftTopicStreamReader::init(std::vector<SingleTopicReader> readers) {
    if (!checkTopicStream(readers)) {
        return protocol::ERROR_CLIENT_INIT_INVALID_PARAMETERS;
    }
    _readers = std::move(readers);
    if (!moveToNextTopic()) {
        return protocol::ERROR_CLIENT_INIT_INVALID_PARAMETERS;
    }
    return protocol::ERROR_NONE;
}

protocol::ErrorCode SwiftTopicStreamReader::init(const SwiftReaderConfig &config) { return protocol::ERROR_NONE; }

protocol::ErrorCode SwiftTopicStreamReader::readLoop(std::function<protocol::ErrorCode(SwiftReader *)> fn) {
    protocol::ErrorCode ec = protocol::ERROR_CLIENT_NO_MORE_MESSAGE;
    while (_currentReader != nullptr) {
        ec = fn(_currentReader->getImpl());
        if (ec != protocol::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT) {
            if (ec != protocol::ERROR_NONE) {
                AUTIL_LOG(DEBUG,
                          "topic id:%d, topic name:%s, error: %s",
                          _currentIdx,
                          _currentReader->topicName.c_str(),
                          protocol::ErrorCode_Name(ec).c_str());
            }
            break;
        }
        AUTIL_LOG(
            INFO, "topic id: %d, topic name: %s eof, move to next", _currentIdx, _currentReader->topicName.c_str());
        if (!moveToNextTopic()) {
            ec = protocol::ERROR_CLIENT_EOF;
            break;
        }
    }
    return ec;
}

protocol::ErrorCode SwiftTopicStreamReader::read(protocol::Message &msg, int64_t timeout) {
    auto fn = [&msg, timeout](SwiftReader *reader) { return reader->read(msg, timeout); };
    return readLoop(std::move(fn));
}

protocol::ErrorCode SwiftTopicStreamReader::read(protocol::Messages &msgs, int64_t timeout) {
    auto fn = [&msgs, timeout](SwiftReader *reader) { return reader->read(msgs, timeout); };
    return readLoop(std::move(fn));
}

protocol::ErrorCode SwiftTopicStreamReader::read(int64_t &timeStamp, protocol::Message &msg, int64_t timeout) {
    auto fn = [&timeStamp, &msg, timeout](SwiftReader *reader) { return reader->read(timeStamp, msg, timeout); };
    return readLoop(std::move(fn));
}

protocol::ErrorCode SwiftTopicStreamReader::read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout) {
    auto fn = [&timeStamp, &msgs, timeout](SwiftReader *reader) { return reader->read(timeStamp, msgs, timeout); };
    return readLoop(std::move(fn));
}

protocol::ErrorCode SwiftTopicStreamReader::seekByMessageId(int64_t msgId) {
    // not support now
    return protocol::ERROR_CLIENT_UNSUPPORTED_METHOD;
}

protocol::ErrorCode SwiftTopicStreamReader::seekByTimestamp(int64_t timestamp, bool force) {
    if (timestamp < 0 || timestamp == std::numeric_limits<int64_t>::max()) {
        AUTIL_LOG(ERROR, "invalid timestamp: %ld", timestamp);
        return protocol::ERROR_CLIENT_INVALID_PARAMETERS;
    }

    bool found = false;
    for (auto i = 0; i < _readers.size(); ++i) {
        if (_readers[i].contains(timestamp)) {
            _currentReader = &_readers[i];
            _currentIdx = i;
            found = true;
            break;
        }
    }
    if (found) {
        return _currentReader->getImpl()->seekByTimestamp(timestamp, force);
    } else {
        return protocol::ERROR_CLIENT_NO_MORE_MESSAGE;
    }
}

protocol::ErrorCode SwiftTopicStreamReader::seekByProgress(const protocol::ReaderProgress &progress, bool force) {
    if (progress.topicprogress_size() != 1) {
        AUTIL_LOG(ERROR, "stream topic new exactly one topic progress");
        return protocol::ERROR_CLIENT_INVALID_PARAMETERS;
    }
    // seek by topic name
    bool found = false;
    for (auto i = 0; i < _readers.size(); ++i) {
        if (_readers[i].topicName == progress.topicprogress(0).topicname()) {
            _currentIdx = i;
            _currentReader = &_readers[i];
            found = true;
            break;
        }
    }
    if (found) {
        return _currentReader->getImpl()->seekByProgress(progress, force);
    } else {
        AUTIL_LOG(ERROR, "topic %s is not found", progress.topicprogress(0).topicname().c_str());
        return protocol::ERROR_CLIENT_INVALID_PARAMETERS;
    }
}

void SwiftTopicStreamReader::checkCurrentError(protocol::ErrorCode &errorCode, std::string &errorMsg) const {
    if (_currentReader == nullptr) {
        errorCode = protocol::ERROR_CLIENT_EOF;
    } else {
        _currentReader->getImpl()->checkCurrentError(errorCode, errorMsg);
    }
}

SwiftPartitionStatus SwiftTopicStreamReader::getSwiftPartitionStatus() {
    if (_readers.size() > 0) {
        return _readers.back().getImpl()->getSwiftPartitionStatus();
    } else {
        SwiftPartitionStatus status;
        status.refreshTime = std::numeric_limits<int64_t>::max();
        status.maxMessageId = -1;
        status.maxMessageTimestamp = -1;
        return status;
    }
}

void SwiftTopicStreamReader::setRequiredFieldNames(const std::vector<std::string> &fieldNames) {
    for (auto &it : _readers) {
        it.getImpl()->setRequiredFieldNames(fieldNames);
    }
}

std::vector<std::string> SwiftTopicStreamReader::getRequiredFieldNames() {
    if (_currentReader != nullptr) {
        _currentReader->getImpl()->getRequiredFieldNames();
    }
    return {};
}

void SwiftTopicStreamReader::setFieldFilterDesc(const std::string &filterDesc) {
    for (auto &it : _readers) {
        it.getImpl()->setFieldFilterDesc(filterDesc);
    }
}

void SwiftTopicStreamReader::setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) {
    if (!_readers.empty()) {
        _readers.back().getImpl()->setTimestampLimit(timestampLimit, acceptTimestamp);
    }
}

bool SwiftTopicStreamReader::updateCommittedCheckpoint(int64_t checkpoint) {
    if (_currentReader == nullptr) {
        return false;
    }
    return _currentReader->getImpl()->updateCommittedCheckpoint(checkpoint);
}

common::SchemaReader *SwiftTopicStreamReader::getSchemaReader(const char *data, protocol::ErrorCode &ec) {
    ec = protocol::ERROR_ADMIN_TOPIC_NOT_SUPPORT_SCHEMA;
    return nullptr;
}

protocol::ErrorCode SwiftTopicStreamReader::getReaderProgress(protocol::ReaderProgress &progress) const {
    if (_currentReader == nullptr) {
        return protocol::ERROR_CLIENT_EOF;
    }
    return _currentReader->getImpl()->getReaderProgress(progress);
}

int64_t SwiftTopicStreamReader::getCheckpointTimestamp() const {
    if (_currentReader == nullptr) {
        return -1;
    }
    return _currentReader->getImpl()->getCheckpointTimestamp();
}

bool SwiftTopicStreamReader::checkTopicStream(const std::vector<SingleTopicReader> &readers) const {
    for (size_t i = 0; i < readers.size(); ++i) {
        for (size_t j = i + 1; j < readers.size(); ++j) {
            if (readers[i].topicName == readers[j].topicName) {
                AUTIL_LOG(ERROR, "stream mode  not support topic [%s] more than once", readers[j].topicName.c_str());
                return false;
            }
        }
    }
    for (size_t i = 0; i < readers.size(); ++i) {
        if (!readers[i].check()) {
            AUTIL_LOG(ERROR, "chcek reader [%lu] failed, topic [%s]", i, readers[i].topicName.c_str());
            return false;
        }
        if (i + 1 < readers.size() && !readers[i].isBounded()) {
            AUTIL_LOG(ERROR,
                      "reader [%lu] topicname [%s] is not last reader, but is not bouned",
                      i,
                      readers[i].topicName.c_str());
            return false;
        }
    }
    return std::is_sorted(readers.begin(), readers.end());
}

bool SwiftTopicStreamReader::moveToNextTopic() {
    ++_currentIdx;
    if (_currentIdx < (int)_readers.size()) {
        _currentReader = &_readers[_currentIdx];
        return true;
    } else {
        _currentReader = nullptr;
        return false;
    }
}

std::string SwiftTopicStreamReader::getTopicName(size_t idx) const {
    if (idx < 0 || idx >= _readers.size()) {
        return "";
    }
    return _readers[idx].topicName;
}

} // namespace client
} // namespace swift
