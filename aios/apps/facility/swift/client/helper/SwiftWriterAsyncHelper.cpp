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
#include "swift/client/helper/SwiftWriterAsyncHelper.h"

#include <assert.h>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <stdio.h>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftWriter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace autil;
using namespace autil::result;
using namespace swift::protocol;
using namespace swift::common;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftWriterAsyncHelper);

SwiftWriteCallbackItem::SwiftWriteCallbackItem() : payload(std::make_shared<Payload>()) {}

bool SwiftWriteCallbackItem::onDone(SwiftWriteCallbackParam param) {
    auto callback_ = std::move(payload->callback);
    if (callback_) {
        callback_(std::move(param));
        return true;
    }
    return false;
}

bool SwiftWriteCallbackItem::onTimeout(const std::string &src) {
    auto callback_ = std::move(payload->callback);
    if (callback_) {
        callback_(RuntimeError::make("[%s] async writer timeout, timeoutInUs %ld, checkpoint %ld",
                                     src.c_str(),
                                     payload->timeoutInUs,
                                     checkpointId));
        return true;
    }
    return false;
}

SwiftWriterAsyncHelper::SwiftWriterAsyncHelper()
    : _ckptBuffer(this)
    , _checkLoopNotifier(1) // accumulatedNotificationMax = 1
{}

SwiftWriterAsyncHelper::~SwiftWriterAsyncHelper() { stop(); }

bool SwiftWriterAsyncHelper::init(std::unique_ptr<swift::client::SwiftWriter> writer, const std::string &desc) {
    assert(writer && "writer should not be nullptr");
    {
        char buf[1024];
        snprintf(buf, 1024, "%p,%s %s", this, writer->getTopicName().c_str(), desc.c_str());
        _desc = std::string(buf);
    }

    int64_t checkpointId = writer->getCommittedCheckpointId();
    if (checkpointId != -1) {
        AUTIL_LOG(ERROR, "[%s] do not reuse swift writer, checkpointId[%ld]", _desc.c_str(), checkpointId);
        return false;
    }
    _ckptBuffer.init(_checkpointIdAllocator);

    _writer = std::move(writer);
    _writer->setCommittedCallBack(std::bind(&SwiftWriterAsyncHelper::commitCallback, this, std::placeholders::_1));

    _workerThread = Thread::createThread(std::bind(&SwiftWriterAsyncHelper::workerCheckLoop, this), "SwiftCbLp");
    if (!_workerThread) {
        AUTIL_LOG(ERROR, "[%s] init swift async callback thread failed", _desc.c_str());
        return false;
    }
    return true;
}

void SwiftWriterAsyncHelper::pushToTimeoutQueue(const SwiftWriteCallbackItem &item) { _timeoutQueue.emplace(item); }

void SwiftWriterAsyncHelper::getExpiredItemsFromTimeoutQueue(std::list<SwiftWriteCallbackItem> &result, int64_t now) {
    for (; !_timeoutQueue.empty(); _timeoutQueue.pop()) {
        auto &item = _timeoutQueue.top();
        if (item.expireTimeInUs > now) {
            break;
        }
        result.emplace_back(item);
    }
}

void SwiftWriterAsyncHelper::stop() {
    _writer.reset();
    _checkLoopNotifier.notifyExit();
    _workerThread.reset();
    _stopped = true;
    // no lock needed
    auto checkpointIdQueue = std::move(_checkpointIdQueue);
    auto timeoutQueue = std::move(_timeoutQueue);
    for (; !checkpointIdQueue.empty(); checkpointIdQueue.pop_front()) {
        if (auto item = checkpointIdQueue.front(); item.onTimeout(_desc)) {
            ++_timeoutItemCountOnStop;
        }
    }
    for (; !_timeoutQueue.empty(); _timeoutQueue.pop()) {
        if (auto item = _timeoutQueue.top(); item.onTimeout(_desc)) {
            ++_timeoutItemCountOnStop;
        }
    }
    assert(_pendingItemCount == _timeoutItemCountOnStop && "pending item count mismatch");
    _pendingItemCount = 0;
    _timeoutItemCountOnStop = 0;
}

size_t SwiftWriterAsyncHelper::getPendingItemCount() const {
    ScopedLock _(_lock);
    return _pendingItemCount;
}

void SwiftWriterAsyncHelper::waitEmpty() {
    AUTIL_LOG(INFO, "[%s] start wait empty for topic[%s]", _desc.c_str(), getTopicName().c_str());
    size_t loop;
    for (loop = 0;; ++loop) {
        size_t itemCount = getPendingItemCount();
        if (loop % 200 == 0) { // 2s
            AUTIL_LOG(INFO,
                      "[%s] waiting empty for topic[%s] loop[%ld] pending[%ld]",
                      _desc.c_str(),
                      getTopicName().c_str(),
                      loop,
                      itemCount);
        }
        if (itemCount == 0) {
            break;
        }
        usleep(10 * 1000); // 10ms
    }
    AUTIL_LOG(INFO, "[%s] end wait empty for topic[%s] loop[%ld]", _desc.c_str(), getTopicName().c_str(), loop);
}

void SwiftWriterAsyncHelper::write(MessageInfo *msgInfos,
                                   size_t count,
                                   SwiftWriteCallbackItem::WriteCallbackFunc callback,
                                   int64_t timeoutInUs) {
    assert(!_stopped);
    AUTIL_LOG(TRACE1, "[%s] start write message[%p,%lu]", _desc.c_str(), msgInfos, count);

    if (count == 0) {
        callback(SwiftWriteCallbackParam{nullptr, 0});
        return;
    }
    auto now = TimeUtility::currentTime();
    SwiftWriteCallbackItem item;
    item.expireTimeInUs = TIME_ADD(now, timeoutInUs);
    auto &itemPayload = *item.payload;
    itemPayload.timeoutInUs = timeoutInUs;
    itemPayload.msgInfos = msgInfos;
    itemPayload.count = count;

    ErrorCode ec = protocol::ERROR_NONE;
    auto ckptIdLimit = getCkptIdLimit();
    {
        ScopedLock _(_lock);
        for (size_t i = 0; i < count; ++i) {
            if (_checkpointIdAllocator >= ckptIdLimit) {
                ec = protocol::ERROR_CLIENT_ASYNC_WRITE_BUFFER_FULL;
                goto ERROR_CALLBACK;
            }
            msgInfos[i].checkpointId = _checkpointIdAllocator;
            ec = _writer->write(msgInfos[i]);
            if (ec != protocol::ERROR_NONE) {
                goto ERROR_CALLBACK;
            }
            ++_checkpointIdAllocator;
        }

        auto checkpointId = msgInfos[count - 1].checkpointId;
        item.checkpointId = checkpointId;
        itemPayload.callback = std::move(callback);
        AUTIL_LOG(TRACE1,
                  "[%s] start emplace checkpointId[%ld] message[%p,%lu]",
                  _desc.c_str(),
                  checkpointId,
                  msgInfos,
                  count);
        if (item.expireTimeInUs < std::numeric_limits<int64_t>::max()) {
            // need check timeout
            assert((itemPayload.timeoutInUs <= 60 * 1000 * 1000) && "do not use too large timeout");
            pushToTimeoutQueue(item);
        }
        _checkpointIdQueue.emplace_back(std::move(item));
        ++_pendingItemCount;
        AUTIL_LOG(TRACE1,
                  "[%s] end emplace checkpointId[%ld] message[%p,%lu] pending[%lu]",
                  _desc.c_str(),
                  checkpointId,
                  msgInfos,
                  count,
                  _pendingItemCount);
        return;
    }

ERROR_CALLBACK:
    AUTIL_LOG(ERROR, "[%s] writer has error[%d] message[%p,%lu]", _desc.c_str(), ec, msgInfos, count);
    callback(RuntimeError::make("[%s] writer has error, error code[%d]", _desc.c_str(), ec));
}

void SwiftWriterAsyncHelper::commitCallback(const std::vector<std::pair<int64_t, int64_t>> &commitInfo) {
    if (_ckptBuffer.addTimestamp(commitInfo)) {
        _checkLoopNotifier.notify();
    }
}

void SwiftWriterAsyncHelper::workerCheckLoop() {
    AUTIL_LOG(INFO, "[%s] worker loop started", _desc.c_str());
    while (true) {
        int status = _checkLoopNotifier.waitNotification(50 * 1000); // 50ms
        if (status == Notifier::EXITED) {
            break;
        }
        checkCallback();
    }
    AUTIL_LOG(INFO, "[%s] worker loop finished", _desc.c_str());
}

int64_t SwiftWriterAsyncHelper::getCommittedCkptId() const { return _ckptBuffer.getCommittedId(); }
int64_t SwiftWriterAsyncHelper::getCkptIdLimit() const { return _ckptBuffer.getIdLimit(); }
void SwiftWriterAsyncHelper::stealRange(std::vector<int64_t> &timestamps, int64_t ckptId) {
    _ckptBuffer.stealRange(timestamps, ckptId);
}
std::string SwiftWriterAsyncHelper::getTopicName() const {
    if (_writer) {
        return _writer->getTopicName();
    }
    return "";
}

void SwiftWriterAsyncHelper::checkCallback() {
    assert(!_stopped);
    std::list<SwiftWriteCallbackItem> timeoutList;
    std::list<SwiftWriteCallbackItem> checkpointIdList;
    int64_t checkpointId = getCommittedCkptId();
    AUTIL_LOG(DEBUG, "[%s] start handle for checkpointId[%ld]", _desc.c_str(), checkpointId);

    int64_t now = TimeUtility::currentTime();
    {
        ScopedLock _(_lock);
        for (; !_checkpointIdQueue.empty(); _checkpointIdQueue.pop_front()) {
            auto &item = _checkpointIdQueue.front();
            if (item.checkpointId > checkpointId) {
                break;
            }
            checkpointIdList.emplace_back(std::move(item));
        }
        getExpiredItemsFromTimeoutQueue(timeoutList, now);
    }
    // DO NOT callback in lock
    size_t doneCnt = 0, timeoutCnt = 0;
    thread_local std::vector<int64_t> timestamps;
    for (auto &item : checkpointIdList) {
        timestamps.reserve(item.payload->count);
        stealRange(timestamps, item.checkpointId);
        assert(item.payload->count == timestamps.size());
        AUTIL_LOG(TRACE1,
                  "[%s] onDone for checkpointId[%ld] message[%p,%lu]",
                  _desc.c_str(),
                  checkpointId,
                  item.payload->msgInfos,
                  item.payload->count);
        if (item.onDone({timestamps.data(), timestamps.size()})) {
            ++doneCnt;
        }
    }
    for (auto &item : timeoutList) {
        AUTIL_LOG(TRACE1,
                  "[%s] onTimeout for checkpointId[%ld] message[%p,%lu]",
                  _desc.c_str(),
                  checkpointId,
                  item.payload->msgInfos,
                  item.payload->count);
        if (item.onTimeout(_desc)) {
            ++timeoutCnt;
        }
    }

    size_t itemCount;
    {
        ScopedLock _(_lock);
        itemCount = (_pendingItemCount -= doneCnt + timeoutCnt);
    }
    AUTIL_LOG(DEBUG,
              "[%s] end handle for checkpointId[%ld] done[%lu] timeout[%lu] pending[%lu]",
              _desc.c_str(),
              checkpointId,
              doneCnt,
              timeoutCnt,
              itemCount);
}

} // end namespace client
} // end namespace swift
