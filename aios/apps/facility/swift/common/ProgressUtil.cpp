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
#include "swift/common/ProgressUtil.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>

#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace swift::protocol;

const uint32_t MAX_RANGE_COUNT = 65536;
namespace swift {
namespace common {

AUTIL_LOG_SETUP(swift, ProgressUtil);

ProgressUtil::ProgressUtil() { _rangeVec.resize(MAX_RANGE_COUNT, 0); }

ProgressUtil::~ProgressUtil() {}

bool ProgressUtil::updateProgress(const TopicReaderProgress &topicProgress) {
    if (_topicName.empty()) {
        _topicName = topicProgress.topicname();
        _mask = topicProgress.uint8filtermask();
        _maskResult = topicProgress.uint8maskresult();
    } else {
        if (_topicName != topicProgress.topicname() || _mask != topicProgress.uint8filtermask() ||
            _maskResult != topicProgress.uint8maskresult()) {
            AUTIL_LOG(WARN,
                      "topic progress not same, [%s %d %d] vs [%s %d %d]",
                      _topicName.c_str(),
                      _mask,
                      _maskResult,
                      topicProgress.topicname().c_str(),
                      topicProgress.uint8filtermask(),
                      topicProgress.uint8maskresult());
            return false;
        }
    }
    for (int idx = 0; idx < topicProgress.partprogress_size(); ++idx) {
        const PartReaderProgress &partProgress = topicProgress.partprogress(idx);
        if (partProgress.from() > partProgress.to()) {
            AUTIL_LOG(WARN,
                      "topicname [%s] progress[%s] invalid, ignore",
                      _topicName.c_str(),
                      partProgress.ShortDebugString().c_str());
            continue;
        }
        for (uint32_t idx = partProgress.from(); idx <= partProgress.to(); ++idx) {
            _rangeVec[idx] = partProgress.timestamp();
        }
    }
    return true;
}

int64_t ProgressUtil::getCoverMinTimestamp(uint32_t from, uint32_t to) {
    if (from >= MAX_RANGE_COUNT || to >= MAX_RANGE_COUNT) {
        return 0;
    }
    int64_t timestamp = _rangeVec[from];
    for (uint32_t idx = from + 1; idx <= to; ++idx) {
        if (timestamp > _rangeVec[idx]) {
            timestamp = _rangeVec[idx];
        }
    }
    return timestamp;
}

} // namespace common
} // namespace swift
