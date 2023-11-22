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

#include "swift/client/SwiftWriter.h"
#include "swift/client/helper/SwiftWriterAsyncHelper.h"

namespace swift::client {

class FixedTimeoutSwiftWriterAsyncHelper : public SwiftWriterAsyncHelper {
private:
    using SwiftWriterAsyncHelper::init;
    using SwiftWriterAsyncHelper::write;

public:
    ~FixedTimeoutSwiftWriterAsyncHelper() { cleanTimeoutQueue(); }

public:
    bool init(std::unique_ptr<swift::client::SwiftWriter> writer, const std::string &desc, int timeoutInUs);
    virtual void write(common::MessageInfo *msgInfos, size_t count, SwiftWriteCallbackItem::WriteCallbackFunc callback);

protected:
    void pushToTimeoutQueue(const SwiftWriteCallbackItem &item) override;
    void getExpiredItemsFromTimeoutQueue(std::list<SwiftWriteCallbackItem> &result, int64_t now) override;

private:
    void cleanTimeoutQueue();

private:
    int64_t _timeoutInUs = 0;
    std::deque<SwiftWriteCallbackItem> _fixedTimeoutQueue;
};

bool FixedTimeoutSwiftWriterAsyncHelper::init(std::unique_ptr<swift::client::SwiftWriter> writer,
                                              const std::string &desc,
                                              int timeoutInUs) {
    _timeoutInUs = timeoutInUs;
    return SwiftWriterAsyncHelper::init(std::move(writer), desc);
}

void FixedTimeoutSwiftWriterAsyncHelper::write(common::MessageInfo *msgInfos,
                                               size_t count,
                                               SwiftWriteCallbackItem::WriteCallbackFunc callback) {
    SwiftWriterAsyncHelper::write(msgInfos, count, callback, _timeoutInUs);
}

void FixedTimeoutSwiftWriterAsyncHelper::pushToTimeoutQueue(const SwiftWriteCallbackItem &item) {
    _fixedTimeoutQueue.emplace_back(item);
}

void FixedTimeoutSwiftWriterAsyncHelper::getExpiredItemsFromTimeoutQueue(std::list<SwiftWriteCallbackItem> &result,
                                                                         int64_t now) {
    assert(_timeoutQueue.empty());
    for (; !_fixedTimeoutQueue.empty(); _fixedTimeoutQueue.pop_front()) {
        auto &item = _fixedTimeoutQueue.front();
        if (item.expireTimeInUs > now) {
            break;
        }
        result.emplace_back(std::move(item));
    }
}

void FixedTimeoutSwiftWriterAsyncHelper::cleanTimeoutQueue() {
    assert(_timeoutQueue.empty());
    for (; !_fixedTimeoutQueue.empty(); _fixedTimeoutQueue.pop_front()) {
        if (auto item = _fixedTimeoutQueue.front(); item.onTimeout(_desc)) {
            ++_timeoutItemCountOnStop;
        }
    }
}

SWIFT_TYPEDEF_PTR(FixedTimeoutSwiftWriterAsyncHelper);

} // namespace swift::client
