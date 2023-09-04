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
#include "swift/common/RangeUtil.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>

using namespace std;
namespace swift {
namespace common {
AUTIL_LOG_SETUP(swift, RangeUtil);

static const uint16_t DEFAULT_RANGE_FROM = 0;
static const uint16_t DEFAULT_RANGE_TO = 65535;

struct CompareRange {
    bool operator()(uint16_t val, const std::pair<uint16_t, uint16_t> &range) const { return val <= range.second; }
};

RangeUtil::RangeUtil(uint32_t partitionCount) {
    splitPartitions(_partitions, partitionCount, DEFAULT_RANGE_FROM, DEFAULT_RANGE_TO);
}

RangeUtil::RangeUtil(uint32_t partitionCount, uint32_t rangeCount) {
    splitPartitions(_partitions, partitionCount, DEFAULT_RANGE_FROM, DEFAULT_RANGE_TO);
    splitMergeRange(rangeCount);
}

RangeUtil::~RangeUtil() {}

vector<uint32_t> RangeUtil::getPartitionIds(uint16_t requiredRangeFrom, uint16_t requiredRangeTo) const {
    vector<uint32_t> partIds;
    if (requiredRangeTo < requiredRangeFrom) {
        return partIds;
    }
    for (size_t i = 0; i < _partitions.size(); ++i) {
        if (!((_partitions[i].first > requiredRangeTo) || (requiredRangeFrom > _partitions[i].second))) {
            partIds.push_back((uint32_t)i);
        }
    }
    return partIds;
}

int32_t RangeUtil::getPartitionId(uint16_t hashId) const {
    RangeVec::const_iterator iter = upper_bound(_partitions.begin(), _partitions.end(), hashId, CompareRange());
    if (iter == _partitions.end()) {
        return -1;
    } else {
        return iter - _partitions.begin();
    }
}

uint16_t RangeUtil::getMergeHashId(uint16_t hashId) const {
    RangeVec::const_iterator iter = upper_bound(_mergeRanges.begin(), _mergeRanges.end(), hashId, CompareRange());
    if (iter == _mergeRanges.end()) {
        return hashId;
    } else {
        return iter->second;
    }
}

void RangeUtil::splitMergeRange(uint32_t rangeCount) {
    for (size_t i = 0; i < _partitions.size(); ++i) {
        splitPartitions(_mergeRanges, rangeCount, _partitions[i].first, _partitions[i].second);
    }
}

void RangeUtil::splitPartitions(vector<pair<uint16_t, uint16_t>> &partitions,
                                uint32_t partitionCount,
                                uint16_t defaultFrom,
                                uint16_t defaultTo) {
    if (partitionCount == 0) {
        return;
    }
    uint32_t rangeCount = defaultTo - defaultFrom + 1;
    uint32_t rangeFrom = defaultFrom;
    uint32_t rangeTo = defaultTo;
    uint32_t c = rangeCount / partitionCount;
    uint32_t m = rangeCount % partitionCount;
    uint32_t from = rangeFrom;
    for (uint32_t i = 0; i < partitionCount && from <= rangeTo; ++i) {
        uint32_t to = from + c + (i >= m ? 0 : 1) - 1;
        partitions.push_back(make_pair<uint16_t, uint16_t>(from, to));
        from = to + 1;
    }
}

bool RangeUtil::getPartRange(uint32_t partId, uint32_t &from, uint32_t &to) const {
    if (partId >= _partitions.size()) {
        return false;
    } else {
        from = _partitions[partId].first;
        to = _partitions[partId].second;
        return true;
    }
}

bool RangeUtil::exactlyInOnePart(uint16_t from, uint16_t to, uint32_t &partId) const {
    RangeVec::const_iterator iter = upper_bound(_partitions.begin(), _partitions.end(), from, CompareRange());
    if (iter == _partitions.end()) {
        return false;
    }
    if (iter->first == from && iter->second == to) {
        partId = iter - _partitions.begin();
        return true;
    }
    return false;
}

} // namespace common
} // namespace swift
