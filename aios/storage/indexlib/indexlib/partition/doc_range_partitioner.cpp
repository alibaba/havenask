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
#include "indexlib/partition/doc_range_partitioner.h"

#include <algorithm>

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(index, DocRangePartitioner);

bool DocRangePartitioner::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint,
                                               const index::PartitionInfoPtr& partitionInfo, size_t totalWayCount,
                                               size_t wayIdx, DocIdRangeVector& ranges)
{
    vector<DocIdRangeVector> rangeVectors;
    if (!GetPartedDocIdRanges(rangeHint, partitionInfo, totalWayCount, rangeVectors)) {
        return false;
    }
    ranges = std::move(rangeVectors[wayIdx]);
    return true;
}

bool DocRangePartitioner::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint,
                                               const index::PartitionInfoPtr& partitionInfo, size_t totalWayCount,
                                               std::vector<DocIdRangeVector>& rangeVectors)
{
    if (totalWayCount == 0) {
        IE_LOG(ERROR, "totalWayCount is zero");
        return false;
    }

    size_t totalHitDoc = 0;
    docid_t incDocCount = partitionInfo->GetPartitionMetrics().incDocCount;
    for (const auto& range : rangeHint) {
        if (range.second <= incDocCount) {
            totalHitDoc += range.second - range.first;
        } else {
            totalHitDoc += incDocCount - range.first;
            break;
        }
    }
    size_t baseQuota = totalHitDoc / totalWayCount;
    size_t remaindQuota = totalHitDoc % totalWayCount;

    constexpr double FACTOR = 1.25;
    size_t currentRangeIdx = 0;
    size_t singleWayUsedQuota = 0;
    DocIdRangeVector ranges = rangeHint;
    auto PushRange = [](DocIdRangeVector& vec, const DocIdRange& range) {
        if (range.second > range.first) {
            vec.push_back(range);
        }
    };

    for (size_t wayIdx = 0; wayIdx < totalWayCount; ++wayIdx) {
        DocIdRangeVector singleWayRanges;
        size_t quotaLimit = baseQuota;
        if (wayIdx == 0) {
            quotaLimit += remaindQuota;
        }

        while (currentRangeIdx < ranges.size()) {
            auto& range = ranges[currentRangeIdx];
            if (range.first >= incDocCount) {
                PushRange(singleWayRanges, range);
                ++currentRangeIdx;
                continue;
            }
            auto rangeSize = range.second - range.first;
            if (singleWayUsedQuota + rangeSize < quotaLimit * FACTOR) {
                PushRange(singleWayRanges, range);
                ++currentRangeIdx;
                singleWayUsedQuota += rangeSize;
            } else {
                rangeSize = quotaLimit - singleWayUsedQuota;
                PushRange(singleWayRanges, {range.first, range.first + rangeSize});
                singleWayUsedQuota += rangeSize;
                range.first += rangeSize;
            }
            if (singleWayUsedQuota >= quotaLimit) {
                singleWayUsedQuota -= quotaLimit;
                break;
            }
        }
        if (!singleWayRanges.empty()) {
            if (wayIdx + 1 == totalWayCount) {
                if (currentRangeIdx < ranges.size()) {
                    singleWayRanges.insert(singleWayRanges.end(), ranges.begin() + currentRangeIdx, ranges.end());
                }
            }
            rangeVectors.push_back(std::move(singleWayRanges));
        }
    }

    return true;
}

DocIdRangeVector::const_iterator DocRangePartitioner::IntersectRange(const DocIdRangeVector::const_iterator& begin1,
                                                                     const DocIdRangeVector::const_iterator& end1,
                                                                     const DocIdRangeVector::const_iterator& begin2,
                                                                     const DocIdRangeVector::const_iterator& end2,
                                                                     DocIdRangeVector& output)
{
    auto iter1 = begin1;
    auto iter2 = begin2;

    while (iter1 != end1 && iter2 != end2) {
        if ((*iter1).second <= (*iter2).first) {
            ++iter1;
            continue;
        }
        if ((*iter1).first >= (*iter2).second) {
            ++iter2;
            continue;
        }

        auto left = std::max((*iter1).first, (*iter2).first);
        auto right = std::min((*iter1).second, (*iter2).second);
        output.push_back({left, right});
        if ((*iter1).second == right) {
            ++iter1;
        }
        if ((*iter2).second == right) {
            ++iter2;
        }
    }
    return iter1;
}
}} // namespace indexlib::partition
