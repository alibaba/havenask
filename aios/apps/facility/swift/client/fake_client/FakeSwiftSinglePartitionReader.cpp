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
#include "swift/client/fake_client/FakeSwiftSinglePartitionReader.h"

#include <bits/std_abs.h>
#include <functional>
#include <iosfwd>
#include <stdlib.h>
#include <unistd.h>

#include "swift/client/Notifier.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace swift::protocol;
using namespace swift::network;
namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, FakeSwiftSinglePartitionReader);

FakeSwiftSinglePartitionReader::FakeSwiftSinglePartitionReader(Notifier *notifier,
                                                               SwiftAdminAdapterPtr adminAdapter,
                                                               SwiftReaderConfig config,
                                                               uint32_t partId)
    : SwiftSinglePartitionReader(adminAdapter, SwiftRpcChannelManagerPtr(), partId, config, -1, notifier)
    , _ec(ERROR_NONE)
    , _retryInterval(0)
    , _fillBufferInterval(-100)
    , _autoAddMsg(true)
    , _notifier(notifier) {
    _partitionStatus.refreshTime = -1;
    _partitionStatus.maxMessageId = -1;
    _partitionStatus.maxMessageTimestamp = -1;
    _readOffset = 0;
}

FakeSwiftSinglePartitionReader::~FakeSwiftSinglePartitionReader() { _fillBufferThreadPtr.reset(); }

void FakeSwiftSinglePartitionReader::setErrorCode(ErrorCode ec) { _ec = ec; }

ErrorCode FakeSwiftSinglePartitionReader::seekByTimestamp(int64_t timestamp) {
    if (_ec != ERROR_NONE) {
        return _ec;
    }
    size_t i = 0;
    for (; i < _messages.size(); i++) {
        if (_messages[i].timestamp() >= timestamp) {
            break;
        }
    }
    _readOffset = i;
    setSeekTimestamp(timestamp);
    return ERROR_NONE;
}

int64_t FakeSwiftSinglePartitionReader::tryFillBuffer(int64_t currentTime) {
    (void)currentTime;
    if (_fillBufferThreadPtr || (_ec != ERROR_NONE)) {
        return _retryInterval;
    }
    if (_fillBufferInterval > 0) {
        _fillBufferThreadPtr =
            autil::Thread::createThread(std::bind(&FakeSwiftSinglePartitionReader::doTryFillBuffer, this));
    } else {
        doTryFillBuffer();
    }
    return _retryInterval;
}

void FakeSwiftSinglePartitionReader::doTryFillBuffer() {
    if (_fillBufferInterval > 0) {
        usleep(_fillBufferInterval * 1000);
    }
    if (_autoAddMsg) {
        Message msg;
        msg.set_timestamp(abs(_fillBufferInterval));
        _messages.push_back(msg);
    }
    _notifier->notify();
}

void FakeSwiftSinglePartitionReader::checkCurrentError(ErrorCode &errorCode, string &errorMsg) const {
    errorCode = _ec;
    errorMsg = "error msg";
}

} // namespace client
} // namespace swift
