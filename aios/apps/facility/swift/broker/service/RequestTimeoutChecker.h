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

#include <stddef.h>
#include <stdint.h>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/common/Common.h"

namespace swift::service {
class TopicPartitionSupervisor;
} // namespace swift::service

namespace swift {
namespace service {

class RequestTimeoutChecker {
public:
    RequestTimeoutChecker(TopicPartitionSupervisor *tp, size_t timeoutUs, size_t notifyIntervalMs);
    virtual ~RequestTimeoutChecker();

private:
    RequestTimeoutChecker(const RequestTimeoutChecker &);
    RequestTimeoutChecker &operator=(const RequestTimeoutChecker &);

public:
    int64_t getCheckTime();
    int64_t getRequestTimeout() { return _timeout; }
    // virtual for test
    virtual std::pair<bool, int64_t> currentTime();

private:
    void checkTimeout();

private:
    TopicPartitionSupervisor *_tpSupervisor = nullptr;
    size_t _timeout;
    size_t _notifyInterval;
    autil::ThreadMutex _timeMutex;
    int64_t _lastTime;
    autil::LoopThreadPtr _timeoutCheckThread;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(RequestTimeoutChecker);

} // end namespace service
} // end namespace swift
