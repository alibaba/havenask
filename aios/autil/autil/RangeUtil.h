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
#include <string>
#include <utility>
#include <vector>

namespace autil {

typedef std::pair<uint16_t, uint16_t> PartitionRange;
typedef std::vector<PartitionRange> RangeVec;

class RangeUtil {
private:
    RangeUtil();
    ~RangeUtil();

private:
    RangeUtil(const RangeUtil &);
    RangeUtil &operator=(const RangeUtil &);

public:
    static bool getRange(uint32_t partCount, uint32_t partId, PartitionRange &range);

    static RangeVec splitRange(uint32_t rangeFrom, uint32_t rangeTo, uint32_t partitionCount);

    static int32_t
    getRangeIdx(uint32_t rangeFrom, uint32_t rangeTo, uint32_t partitionCount, const PartitionRange &range);

    static int32_t getRangeIdxByHashId(uint32_t rangeFrom, uint32_t rangeTo, uint32_t partitionCount, uint32_t hashId);

    static std::string getRangeName(const autil::PartitionRange &range);

public:
    static const uint32_t MAX_PARTITION_RANGE;
};

} // namespace autil
