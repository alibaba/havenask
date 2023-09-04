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

#include <cstdint>
#include <limits>
#include <map>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/client/SwiftWriterImpl.h"
#include "swift/common/Common.h"
#include "swift/common/MessageInfo.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace client {

class FakeSwiftWriter : public SwiftWriterImpl {
public:
    FakeSwiftWriter()
        : SwiftWriterImpl(network::SwiftAdminAdapterPtr(),
                          network::SwiftRpcChannelManagerPtr(),
                          autil::ThreadPoolPtr(),
                          protocol::TopicInfo())
        , _sentMsgCount(0)
        , _ec(protocol::ERROR_NONE)
        , _callCnt(0) {}
    ~FakeSwiftWriter() {}

private:
    FakeSwiftWriter(const FakeSwiftWriter &);
    FakeSwiftWriter &operator=(const FakeSwiftWriter &);

public:
    virtual protocol::ErrorCode write(common::MessageInfo &msgInfo) {
        _callCnt++;
        auto it = _errorCodeMap.find(_callCnt);
        if (it != _errorCodeMap.end() && it->second != protocol::ERROR_NONE) {
            return it->second;
        }
        if (protocol::ERROR_NONE != _ec) {
            return _ec;
        }
        _lastMsgInfo = msgInfo;
        autil::ScopedLock lock(_mutex);
        _sentMsgCount++;
        return protocol::ERROR_NONE;
    }
    virtual int64_t getCommittedCheckpointId() const { return std::numeric_limits<int64_t>::max(); }
    virtual int64_t getLastRefreshTime(uint32_t pid) const {
        (void)pid;
        return 0;
    }
    uint32_t getSentMsgCount() { return _sentMsgCount; }

private:
    mutable autil::ThreadMutex _mutex;
    uint32_t _sentMsgCount;
    common::MessageInfo _lastMsgInfo;
    protocol::ErrorCode _ec;
    uint32_t _callCnt;
    std::map<uint32_t, protocol::ErrorCode> _errorCodeMap;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeSwiftWriter);

} // namespace client
} // namespace swift
