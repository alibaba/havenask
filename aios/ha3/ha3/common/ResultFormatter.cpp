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
#include "ha3/common/ResultFormatter.h"

#include <stdint.h>
#include <map>
#include <utility>
#include <vector>

#include "ha3/isearch.h"

using namespace std;

namespace isearch {
namespace common {

double ResultFormatter::getCoveredPercent(
        const Result::ClusterPartitionRanges &ranges)
{
    if (ranges.empty()) {
        return 0.0;
    }
    uint32_t rangeSum = 0;
    for (Result::ClusterPartitionRanges::const_iterator it = ranges.begin();
         it != ranges.end(); ++it)
    {
        const Result::PartitionRanges &oneClusterRanges = it->second;
        for (Result::PartitionRanges::const_iterator it2 = oneClusterRanges.begin();
             it2 != oneClusterRanges.end(); ++it2)
        {
            rangeSum += (it2->second - it2->first + 1);
        }
    }

    return 100.0 * rangeSum / MAX_PARTITION_COUNT / ranges.size();
}

} // namespace common
} // namespace isearch
