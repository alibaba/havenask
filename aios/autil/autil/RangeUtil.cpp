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
#include "autil/RangeUtil.h"

#include <ext/alloc_traits.h>
#include <stddef.h>
#include <cassert>
#include <iosfwd>
#include <type_traits>
#include <memory>

using namespace std;

namespace autil {

const uint32_t RangeUtil::MAX_PARTITION_RANGE = 65535;

static vector<RangeVec> initRangeVecTable(uint32_t maxPart) {
    vector<RangeVec> rangeVecTable;
    rangeVecTable.push_back(RangeVec());
    for (uint32_t i = 1; i <= maxPart; i++) {
        rangeVecTable.emplace_back(RangeUtil::splitRange(0, RangeUtil::MAX_PARTITION_RANGE, i));
    }
    return rangeVecTable;
}

static const vector<RangeVec> globalRangeVecTable = initRangeVecTable(512);


RangeUtil::RangeUtil() {
}

RangeUtil::~RangeUtil() {
}

bool RangeUtil::getRange(uint32_t partCount, uint32_t partId, PartitionRange &range) {
    if (partId >= partCount || partCount > MAX_PARTITION_RANGE + 1) {
        return false;
    }
    if (partCount < globalRangeVecTable.size()) {
        range = globalRangeVecTable[partCount][partId];
        return true;
    }

    const RangeVec &vec = RangeUtil::splitRange(0, MAX_PARTITION_RANGE, partCount);
    if (partId >= vec.size()) {
        return false;
    }
    range = vec[partId];
    return true;
}

RangeVec RangeUtil::splitRange(uint32_t rangeFrom, uint32_t rangeTo, uint32_t partitionCount)
{
    assert(rangeTo <= MAX_PARTITION_RANGE);
    if (rangeFrom == 0 && rangeTo == MAX_PARTITION_RANGE
        && partitionCount < globalRangeVecTable.size())
    {
        return globalRangeVecTable[partitionCount];
    }
    RangeVec ranges;
    if (partitionCount == 0 || rangeFrom > rangeTo) {
        return ranges;
    }
    uint32_t rangeCount = rangeTo - rangeFrom + 1;
    uint32_t c = rangeCount / partitionCount;
    uint32_t m = rangeCount % partitionCount;
    uint32_t from = rangeFrom;
    for (uint32_t i = 0; i < partitionCount && from <= rangeTo; ++i) {
        uint32_t to = from + c + (i >= m ? 0 : 1) - 1;
        ranges.push_back(make_pair(from, to));
        from = to + 1;
    }
    return ranges;
}

int32_t RangeUtil::getRangeIdx(uint32_t rangeFrom, uint32_t rangeTo,
                               uint32_t partitionCount, const PartitionRange &range)
{
    auto rangeVec = splitRange(rangeFrom, rangeTo, partitionCount);
    if (rangeVec.size() == 0) {
        return -1;
    }
    for (size_t i = 0 ; i < rangeVec.size(); i++) {
        if (rangeVec[i].first == range.first && rangeVec[i].second == range.second) {
            return i;
        }
    }
    return -1;
}

int32_t RangeUtil::getRangeIdxByHashId(uint32_t rangeFrom, uint32_t rangeTo, uint32_t partitionCount, uint32_t hashId) {
    assert(rangeFrom <=rangeTo && rangeTo <= MAX_PARTITION_RANGE && "invalid range");
    assert(partitionCount > 0 && partitionCount <= (rangeTo - rangeFrom + 1) && "invalid partitionCount");
    uint32_t rangeCount = rangeTo - rangeFrom + 1;
    uint32_t c = rangeCount / partitionCount;
    uint32_t m = rangeCount % partitionCount;
    uint32_t bound = m * (c + 1) + rangeFrom;
    if (hashId >= bound) {
        return (hashId - bound) / c + m;
    } else {
        return (hashId - rangeFrom) / (c + 1);
    }
}

}
