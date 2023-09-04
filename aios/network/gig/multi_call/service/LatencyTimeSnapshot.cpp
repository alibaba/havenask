/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2019-05-28 17:52
 * Author Name: 夕奇
 * Author Email: xiqi.lq@alibaba-inc.com
 */

#include "aios/network/gig/multi_call/service/LatencyTimeSnapshot.h"

using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, LatencyTimeSnapshot);

LatencyTimeSnapshot::LatencyTimeSnapshot() {
    _latencyMap.reset(new LatencyTimeWindowMap());
}

LatencyTimeSnapshot::~LatencyTimeSnapshot() {
}

void LatencyTimeSnapshot::updateLatencyTimeWindow(const std::string &bizName, int64_t windowSize) {
    if (0 == windowSize) {
        ScopedReadWriteLock lock(_mapLock, 'w');
        _latencyMap->erase(bizName);
    } else {
        auto latencyTimeWindow = getLatencyTimeWindow(bizName);
        ScopedReadWriteLock lock(_mapLock, 'w');
        if (!latencyTimeWindow) {
            auto it = _latencyMap->find(bizName);
            if (it != _latencyMap->end()) {
                latencyTimeWindow = it->second;
            }
        }
        if (latencyTimeWindow) {
            latencyTimeWindow->setWindowSize(windowSize);
        } else {
            latencyTimeWindow.reset(new LatencyTimeWindow(windowSize));
            (*_latencyMap)[bizName] = latencyTimeWindow;
        }
    }
}

LatencyTimeWindowPtr LatencyTimeSnapshot::getLatencyTimeWindow(const std::string &bizName) {
    ScopedReadWriteLock lock(_mapLock, 'r');
    auto it = _latencyMap->find(bizName);
    if (it != _latencyMap->end()) {
        return it->second;
    } else {
        return LatencyTimeWindowPtr();
    }
}

int64_t LatencyTimeSnapshot::getAvgLatency(const std::string &bizName) {
    auto latencyTimeWindow = getLatencyTimeWindow(bizName);
    if (latencyTimeWindow) {
        return latencyTimeWindow->getAvgLatency();
    } else {
        return 0;
    }
}

int64_t LatencyTimeSnapshot::pushLatency(const std::string &bizName, int64_t latency) {
    auto latencyTimeWindow = getLatencyTimeWindow(bizName);
    if (latencyTimeWindow) {
        return latencyTimeWindow->push(latency);
    } else {
        return -1;
    }
}

} // namespace multi_call
