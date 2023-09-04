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
#include "swift/broker/storage/TimestampAllocator.h"

#include <algorithm>

#include "autil/TimeUtility.h"

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, TimestampAllocator);

TimestampAllocator::TimestampAllocator(int64_t timeOffset) {
    _lastCurTime = autil::TimeUtility::currentTime() + timeOffset;
}

TimestampAllocator::~TimestampAllocator() {}

int64_t TimestampAllocator::getNextMsgTimestamp(int64_t lastMsgTimestamp) {
    return std::max(getCurrentTimestamp(), lastMsgTimestamp + 1);
}

int64_t TimestampAllocator::getCurrentTimestamp() {
    int64_t curTime = autil::TimeUtility::currentTime();
    if (curTime > _lastCurTime) {
        _lastCurTime = curTime;
    }
    return _lastCurTime;
}

} // namespace storage
} // namespace swift
