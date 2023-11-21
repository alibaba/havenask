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
#include "aios/network/gig/multi_call/stream/GigStreamBase.h"

using namespace std;

namespace multi_call {

AUTIL_DECLARE_AND_SETUP_LOGGER(multi_call, GigStreamBase);

PartMessageQueue::PartMessageQueue() : _receiving(false), _count(0), _ec(MULTI_CALL_ERROR_NONE) {
}

PartMessageQueue::~PartMessageQueue() {
}

bool PartMessageQueue::beginReceive() {
    bool expect = false;
    return _receiving.compare_exchange_weak(expect, true);
}

bool PartMessageQueue::endReceive() {
    bool expect = true;
    return _receiving.compare_exchange_weak(expect, false);
}

bool PartMessageQueue::nextMessage(GigStreamMessage &message, MultiCallErrorCode &ec) {
    autil::ScopedLock lock(_lock);
    if (_messageList.empty()) {
        return false;
    }
    message = _messageList.front();
    _messageList.pop_front();
    if (_messageList.empty()) {
        ec = _ec;
    } else {
        ec = MULTI_CALL_ERROR_NONE;
    }
    _count--;
    return true;
}

bool PartMessageQueue::peekMessage(GigStreamMessage &message, MultiCallErrorCode &ec) {
    autil::ScopedLock lock(_lock);
    if (_messageList.empty()) {
        return false;
    }
    message = _messageList.front();
    ec = _ec;
    if (1u == _messageList.size()) {
        ec = _ec;
    } else {
        ec = MULTI_CALL_ERROR_NONE;
    }
    return true;
}

void PartMessageQueue::appendMessage(const GigStreamMessage &message, MultiCallErrorCode ec) {
    autil::ScopedLock lock(_lock);
    _messageList.push_back(message);
    _ec = ec;
    _count++;
}

GigStreamBase::GigStreamBase() {
}

GigStreamBase::~GigStreamBase() {
}

bool GigStreamBase::hasError() const {
    return false;
}

bool GigStreamBase::innerReceive(const GigStreamMessage &message, MultiCallErrorCode ec) {
    if (!_asyncMode) {
        if (MULTI_CALL_ERROR_NONE == ec) {
            return receive(message);
        } else {
            receiveCancel(message, ec);
            return true;
        }
    } else {
        return appendMessage(message, ec);
    }
}

bool GigStreamBase::appendMessage(const GigStreamMessage &message, MultiCallErrorCode ec) {
    auto partId = message.partId;
    auto &partQueue = getPartQueue(partId);
    partQueue.appendMessage(message, ec);
    return tryNotify(partId);
}

bool GigStreamBase::tryNotify(PartIdTy partId) {
    auto &partQueue = getPartQueue(partId);
    if (partQueue.empty()) {
        return true;
    }
    if (partQueue.beginReceive()) {
        return notifyReceive(partId);
    }
    return true;
}

bool GigStreamBase::asyncReceive(PartIdTy partId) {
    auto &partQueue = getPartQueue(partId);
    while (true) {
        if (!doReceive(partQueue)) {
            return false;
        }
        partQueue.endReceive();
        MULTI_CALL_MEMORY_BARRIER();
        if (!partQueue.empty()) {
            if (!partQueue.beginReceive()) {
                break;
            }
        } else {
            break;
        }
    }
    return true;
}

bool GigStreamBase::doReceive(PartMessageQueue &partQueue) {
    while (true) {
        if (partQueue.empty()) {
            break;
        }
        GigStreamMessage message;
        MultiCallErrorCode ec;
        if (partQueue.nextMessage(message, ec)) {
            if (MULTI_CALL_ERROR_NONE == ec) {
                if (!receive(message)) {
                    return false;
                }
            } else {
                receiveCancel(message, ec);
            }
        } else {
            continue;
        }
    }
    return true;
}

bool GigStreamBase::peekMessage(PartIdTy partId, GigStreamMessage &message,
                                MultiCallErrorCode &ec)
{
    auto &partQueue = getPartQueue(partId);
    if (partQueue.empty()) {
        return false;
    }
    return partQueue.peekMessage(message, ec);
}

PartMessageQueue &GigStreamBase::getPartQueue(PartIdTy partId) {
    {
        autil::ScopedReadLock lock(_asyncQueueLock);
        auto it = _partAsyncQueues.find(partId);
        ;
        if (_partAsyncQueues.end() != it) {
            return it->second;
        }
    }
    {
        autil::ScopedWriteLock lock(_asyncQueueLock);
        auto it = _partAsyncQueues.find(partId);
        ;
        if (_partAsyncQueues.end() != it) {
            return it->second;
        }
        return _partAsyncQueues[partId];
    }
}

} // namespace multi_call
