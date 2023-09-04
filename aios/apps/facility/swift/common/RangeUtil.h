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

#include <cstdint>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace common {

class RangeUtil {
public:
    RangeUtil(uint32_t partitionCount);
    RangeUtil(uint32_t partitionCount, uint32_t rangeCount);
    ~RangeUtil();

private:
    RangeUtil(const RangeUtil &);
    RangeUtil &operator=(const RangeUtil &);

public:
    std::vector<uint32_t> getPartitionIds(uint16_t requiredRangeFrom, uint16_t requiredRangeTo) const;

    int32_t getPartitionId(uint16_t hashId) const;
    uint16_t getMergeHashId(uint16_t hashId) const;
    bool getPartRange(uint32_t partId, uint32_t &from, uint32_t &to) const;
    bool exactlyInOnePart(uint16_t from, uint16_t to, uint32_t &partId) const;

private:
    void splitPartitions(std::vector<std::pair<uint16_t, uint16_t>> &partitions,
                         uint32_t partitionCount,
                         uint16_t defaultFrom,
                         uint16_t defaultTo);
    void splitMergeRange(uint32_t rangeCount);

private:
    typedef std::vector<std::pair<uint16_t, uint16_t>> RangeVec;
    RangeVec _partitions;
    RangeVec _mergeRanges;

private:
    friend class RangeUtilTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(RangeUtil);

} // namespace common
} // namespace swift
