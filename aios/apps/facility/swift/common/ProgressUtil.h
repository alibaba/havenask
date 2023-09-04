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

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class TopicReaderProgress;
} // namespace protocol

namespace common {

class ProgressUtil {
public:
    ProgressUtil();
    ~ProgressUtil();

private:
    ProgressUtil(const ProgressUtil &);
    ProgressUtil &operator=(const ProgressUtil &);

public:
    bool updateProgress(const protocol::TopicReaderProgress &input);
    int64_t getCoverMinTimestamp(uint32_t from, uint32_t to);

private:
    std::vector<int64_t> _rangeVec;
    std::string _topicName;
    uint32_t _mask = 0;
    uint32_t _maskResult = 0;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ProgressUtil);

} // namespace common
} // namespace swift
