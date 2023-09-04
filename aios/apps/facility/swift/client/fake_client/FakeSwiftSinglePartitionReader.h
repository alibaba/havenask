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

#include <algorithm>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftSinglePartitionReader.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace client {
class Notifier;

class FakeSwiftSinglePartitionReader : public SwiftSinglePartitionReader {
public:
    FakeSwiftSinglePartitionReader(Notifier *notifier,
                                   network::SwiftAdminAdapterPtr adminAdapter,
                                   SwiftReaderConfig config = SwiftReaderConfig(),
                                   uint32_t partId = 0);
    ~FakeSwiftSinglePartitionReader();

private:
public:
    protocol::ErrorCode read(protocol::Message &msg, int64_t timeout = 3 * 1000000) {
        (void)timeout;
        if (_messages.empty() || _readOffset >= _messages.size()) {
            return protocol::ERROR_CLIENT_NO_MORE_MESSAGE;
        }

        msg = _messages[_readOffset++];
        return protocol::ERROR_NONE;
    }

    bool tryRead(protocol::Messages &msgs) {
        if (_messages.empty() || _readOffset >= _messages.size()) {
            return false;
        }
        int32_t batchReadCount = _config.batchReadCount;
        int32_t readCount = 0;
        for (; _readOffset < _messages.size() && readCount++ < batchReadCount;) {
            _lastMsgTimestamp = _messages[_readOffset].timestamp();
            *msgs.add_msgs() = _messages[_readOffset++];
        }
        return true;
    }

    void setErrorCode(protocol::ErrorCode ec);

    protocol::ErrorCode seekByTimestamp(int64_t timestamp);

    void push(protocol::Message msg) {
        _messages.push_back(msg);
        setNextTimestamp(msg.timestamp() + 1);
    }

    int64_t getFirstMsgTimestamp() {
        if (_messages.empty() || _readOffset >= _messages.size()) {
            return -1;
        }
        return _messages[_readOffset].timestamp();
    }

    void setTryFillBufferInterval(int64_t retryInterval) { _retryInterval = retryInterval; }
    void setFillBufferInterval(int64_t interval) { _fillBufferInterval = interval; }
    int64_t tryFillBuffer(int64_t currentTime = autil::TimeUtility::currentTime());
    int64_t getUnReadMsgCount() { return (int64_t)_messages.size() - _readOffset; }
    protocol::ErrorCode reportFatalError(bool resetLastError = true) {
        if (_ec == protocol::ERROR_BROKER_NO_DATA) {
            return protocol::ERROR_NONE;
        }
        return _ec;
    }
    void checkCurrentError(protocol::ErrorCode &errorCode, std::string &errorMsg) const;

    SwiftPartitionStatus getSwiftPartitionStatus(int64_t timestamp) {
        (void)timestamp;
        return _partitionStatus;
    }
    void setSwiftPartitionStatus(SwiftPartitionStatus status) { _partitionStatus = status; }
    void setAutoAddMsg(bool value) { _autoAddMsg = value; }

private:
    void doTryFillBuffer();

public:
    std::vector<protocol::Message> _messages;

private:
    size_t _readOffset;
    protocol::ErrorCode _ec;
    int64_t _retryInterval;
    int64_t _fillBufferInterval; // ms
    bool _autoAddMsg;
    SwiftPartitionStatus _partitionStatus;
    Notifier *_notifier;
    autil::ThreadPtr _fillBufferThreadPtr;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeSwiftSinglePartitionReader);

} // namespace client
} // namespace swift
