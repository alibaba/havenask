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

#include <functional>
#include <limits>
#include <memory>
#include <queue>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/Thread.h"
#include "autil/result/Result.h"
#include "swift/client/helper/CheckpointBuffer.h"
#include "swift/common/Common.h"

namespace swift {

namespace common {
class MessageInfo;
} // namespace common

namespace client {

class SwiftWriter;

struct SwiftWriteCallbackParam {
    const int64_t *tsData = nullptr;
    size_t tsCount = 0;
};

struct SwiftWriteCallbackItem {
public:
    typedef std::function<void(autil::result::Result<SwiftWriteCallbackParam>)> WriteCallbackFunc;

public:
    bool onDone(SwiftWriteCallbackParam param);

    bool onTimeout(const std::string &src);

public:
    WriteCallbackFunc callback;
    int64_t expireTimeInUs = 0;
    int64_t checkpointId = 0;
    common::MessageInfo *msgInfos = nullptr; // only for log
    size_t count = 0;
    int64_t timeoutInUs = 0;
};

SWIFT_TYPEDEF_PTR(SwiftWriteCallbackItem);

struct SwiftWriteCallbackItemPtrCmpCheckpointId {
    bool operator()(const SwiftWriteCallbackItemPtr &lhs, const SwiftWriteCallbackItemPtr &rhs) {
        return lhs->checkpointId > rhs->checkpointId;
    }
};

struct SwiftWriteCallbackItemPtrCmpExpireTime {
    bool operator()(const SwiftWriteCallbackItemPtr &lhs, const SwiftWriteCallbackItemPtr &rhs) {
        return lhs->expireTimeInUs > rhs->expireTimeInUs;
    }
};

class SwiftWriterAsyncHelper {
public:
    SwiftWriterAsyncHelper();
    virtual ~SwiftWriterAsyncHelper();

private:
    SwiftWriterAsyncHelper(const SwiftWriterAsyncHelper &);
    SwiftWriterAsyncHelper &operator=(const SwiftWriterAsyncHelper &);

public:
    bool init(std::unique_ptr<swift::client::SwiftWriter> writer, const std::string &desc = "");

public:
    virtual void write(common::MessageInfo *msgInfos,
                       size_t count,
                       SwiftWriteCallbackItem::WriteCallbackFunc callback,
                       int64_t timeout = std::numeric_limits<int64_t>::max());

    size_t getPendingItemCount() const;
    void waitEmpty();
    std::string getTopicName() const;

private:
    void stop();
    void workerCheckLoop();
    void checkCallback();
    void commitCallback(const std::vector<std::pair<int64_t, int64_t>> &commitInfo);

private:
    virtual int64_t getCommittedCkptId() const;                                // virtual for test
    virtual int64_t getCkptIdLimit() const;                                    // virtual for test
    virtual void stealRange(std::vector<int64_t> &timestamps, int64_t ckptId); // virtual for test

private:
    bool _stopped = false;
    CheckpointBuffer _ckptBuffer;
    autil::Notifier _checkLoopNotifier;
    autil::ThreadPtr _workerThread;
    mutable autil::ThreadMutex _lock;
    int64_t _checkpointIdAllocator = 0;
    size_t _pendingItemCount = 0;
    std::priority_queue<SwiftWriteCallbackItemPtr,
                        std::vector<SwiftWriteCallbackItemPtr>,
                        SwiftWriteCallbackItemPtrCmpCheckpointId>
        _checkpointIdQueue;
    std::priority_queue<SwiftWriteCallbackItemPtr,
                        std::vector<SwiftWriteCallbackItemPtr>,
                        SwiftWriteCallbackItemPtrCmpExpireTime>
        _timeoutQueue;
    std::unique_ptr<swift::client::SwiftWriter> _writer;
    std::string _desc;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftWriterAsyncHelper);

} // end namespace client
} // end namespace swift
