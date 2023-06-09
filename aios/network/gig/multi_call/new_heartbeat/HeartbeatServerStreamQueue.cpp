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
#include "multi_call/new_heartbeat/HeartbeatServerStreamQueue.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatServerStreamQueue);

HeartbeatServerStreamQueue::HeartbeatServerStreamQueue(size_t id)
    : _id(id)
    , _run(true)
{
}

HeartbeatServerStreamQueue::~HeartbeatServerStreamQueue() {
    stop();
}

bool HeartbeatServerStreamQueue::init() {
    auto threadName = "GigServerHB" + std::to_string(_id);
    auto thread = autil::Thread::createThread(std::bind(&HeartbeatServerStreamQueue::flush, this),
                                              threadName);
    if (!thread) {
        AUTIL_LOG(ERROR, "create heartbeat thread [%lu] failed", _id);
        return false;
    }
    _thread = thread;
    return true;
}

void HeartbeatServerStreamQueue::stop() {
    {
        autil::ScopedLock lock(_threadCond);
        _run = false;
    }
    notify();
    _thread.reset();
    auto streamMapPtr = getStreamMap();
    if (!streamMapPtr) {
        return;
    }
    const auto &streamMap = *streamMapPtr;
    std::vector<int64_t> streamVec;
    for (const auto &pair : streamMap) {
        auto clientId = pair.first;
        streamVec.push_back(clientId);
    }
    removeStream(streamVec);
}

void HeartbeatServerStreamQueue::notify() {
    autil::ScopedLock lock(_threadCond);
    _threadCond.signal();
}

void HeartbeatServerStreamQueue::flush() {
    while (true) {
        {
            autil::ScopedLock lock(_threadCond);
            if (!_run) {
                break;
            }
            _threadCond.wait();
        }
        if (!_run) {
            break;
        }
        doFlush();
    }
}

void HeartbeatServerStreamQueue::doFlush() {
    auto streamMapPtr = getStreamMap();
    if (!streamMapPtr) {
        return;
    }
    const auto &streamMap = *streamMapPtr;
    std::vector<int64_t> failedStreamVec;
    for (const auto &pair : streamMap) {
        auto clientId = pair.first;
        const auto &stream = pair.second;
        if (!stream->flush(true)) {
            failedStreamVec.push_back(clientId);
        }
    }
    removeStream(failedStreamVec);
}

HeartbeatServerStreamMapPtr HeartbeatServerStreamQueue::getStreamMap() const {
    autil::ScopedLock lock(_streamMutex);
    return _streamMap;
}

void HeartbeatServerStreamQueue::setStreamMap(const HeartbeatServerStreamMapPtr &newMap) {
    autil::ScopedLock lock(_streamMutex);
    _streamMap = newMap;
}

void HeartbeatServerStreamQueue::addStream(const HeartbeatServerStreamPtr &stream) {
    auto oldMap = getStreamMap();
    auto newMap = std::make_shared<HeartbeatServerStreamMap>();
    if (oldMap) {
        for (const auto &pair : *oldMap) {
            const auto &stream = pair.second;
            if (stream->cancelled()) {
                stream->stop();
                continue;
            }
            newMap->emplace(pair);
        }
    }
    newMap->emplace(stream->getClientId(), stream);
    setStreamMap(newMap);
}

void HeartbeatServerStreamQueue::removeStream(const std::vector<int64_t> &streamVec) {
    auto oldMap = getStreamMap();
    HeartbeatServerStreamMapPtr newMap;
    if (oldMap) {
        newMap = std::make_shared<HeartbeatServerStreamMap>(*oldMap);
    } else {
        newMap = std::make_shared<HeartbeatServerStreamMap>();
    }
    for (const auto &clientId : streamVec) {
        auto it = newMap->find(clientId);
        if (it == newMap->end()) {
            continue;
        }
        const auto &stream = it->second;
        stream->stop();
        newMap->erase(it);
    }
    setStreamMap(newMap);
}

}
