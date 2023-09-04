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

#include <assert.h>
#include <memory>
#include <queue>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"

namespace swift {

namespace monitor {
struct WriteMetricsCollector;

typedef std::shared_ptr<WriteMetricsCollector> WriteMetricsCollectorPtr;
} // namespace monitor

namespace util {
class ProductionLogClosure;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {

template <typename RequestType, typename ResponseType>
struct RequestItemInternal {
    swift::util::ProductionLogClosure *closure;
    int64_t receivedTime;
    uint64_t msgCount;
    uint64_t dataSize;
    monitor::WriteMetricsCollectorPtr collector;

    RequestItemInternal() {
        closure = NULL;
        receivedTime = 0;
        msgCount = 0;
        dataSize = 0;
    }
};

template <typename RequestType, typename ResponseType>
class RequestItemQueue {
public:
    typedef RequestItemInternal<RequestType, ResponseType> RequestItem;
    static const uint32_t DEFAULT_REQUESTITEM_LIMIT = 5000;
    static const uint64_t DEFAULT_DATASIZE_LIMIT = 500 * 1024 * 1024; // 500 M
public:
    RequestItemQueue(uint32_t requestItemLimit = DEFAULT_REQUESTITEM_LIMIT,
                     uint64_t dataSizeLimit = DEFAULT_DATASIZE_LIMIT) {
        _requestItemLimit = requestItemLimit;
        _dataSizeLimit = dataSizeLimit;
        _currDataSize = 0;
    };
    ~RequestItemQueue() {
        while (!_requestItemQueue.empty()) {
            RequestItem *item = _requestItemQueue.front();
            DELETE_AND_SET_NULL(item);
            _requestItemQueue.pop();
        }
    };

private:
    RequestItemQueue(const RequestItemQueue &);
    RequestItemQueue &operator=(const RequestItemQueue &);

public:
    bool pushRequestItem(RequestItem *requestItem);
    void popRequestItem(std::vector<RequestItem *> &requestItemVec, uint32_t itemNum) {
        autil::ScopedLock lock(_queueLock);
        uint32_t queueSize = (uint32_t)_requestItemQueue.size();
        uint32_t popItemNum = itemNum > (uint32_t)queueSize ? queueSize : itemNum;
        requestItemVec.clear();
        requestItemVec.reserve(popItemNum);
        for (uint32_t i = 0; i < popItemNum; ++i) {
            requestItemVec.push_back(_requestItemQueue.front());
            _currDataSize -= _requestItemQueue.front()->dataSize;
            _requestItemQueue.pop();
        }
    }
    uint32_t getQueueSize() const {
        autil::ScopedLock lock(_queueLock);
        return (uint32_t)_requestItemQueue.size();
    }

    uint64_t getQueueDataSize() const {
        autil::ScopedLock lock(_queueLock);
        return _currDataSize;
    }
    int64_t getMaxWaitTime() const {
        autil::ScopedLock lock(_queueLock);
        if (_requestItemQueue.size() == 0) {
            return 0;
        } else {
            int64_t curTime = autil::TimeUtility::currentTime();
            int64_t oldestTime = _requestItemQueue.front()->receivedTime;
            return curTime - oldestTime;
        }
    }

public:
    // for test
    void setRequestItemLimit(uint32_t requestItemLimit) { _requestItemLimit = requestItemLimit; }
    void setDataSizeLimit(uint64_t dataSizeLimit) { _dataSizeLimit = dataSizeLimit; }

private:
    std::queue<RequestItem *> _requestItemQueue;
    mutable ::autil::ThreadMutex _queueLock;
    uint32_t _requestItemLimit;
    uint64_t _dataSizeLimit;
    uint64_t _currDataSize;

private:
    friend class RequestItemQueueTest;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(swift, RequestItemQueue, RequestType, ResponseType);

template <typename RequestType, typename ResponseType>
bool RequestItemQueue<RequestType, ResponseType>::pushRequestItem(RequestItem *requestItem) {
    assert(requestItem);
    autil::ScopedLock lock(_queueLock);
    if (((uint32_t)_requestItemQueue.size() + 1 > _requestItemLimit) ||
        (_currDataSize + requestItem->dataSize > _dataSizeLimit)) {
        AUTIL_LOG(WARN,
                  "Broker busy, requestItemQueue size[%d], "
                  "requestItemLimit[%d], currDataSize add requestDataSize[%ld],"
                  " dataSizeLimit[%ld]",
                  (uint32_t)_requestItemQueue.size(),
                  _requestItemLimit,
                  _currDataSize + requestItem->dataSize,
                  _dataSizeLimit);
        return false;
    }
    requestItem->receivedTime = autil::TimeUtility::currentTime();
    _requestItemQueue.push(requestItem);
    _currDataSize += requestItem->dataSize;
    return true;
}

} // namespace storage
} // namespace swift
