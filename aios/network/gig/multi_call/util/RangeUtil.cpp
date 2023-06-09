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
#include "aios/network/gig/multi_call/util/RangeUtil.h"
#include "autil/StringUtil.h"
#include <assert.h>

using namespace std;
using namespace autil;
AUTIL_DECLARE_AND_SETUP_LOGGER(multi_call, RangeUtil);
namespace multi_call {

RangeUtil::RangeUtil(uint32_t partCount, uint32_t rangeFrom, uint32_t rangeTo) {
    _ranges = splitRange(partCount, rangeFrom, rangeTo);
}

RangeUtil::~RangeUtil() {}

const vector<Range> &RangeUtil::getRanges() { return _ranges; }

bool RangeUtil::getRange(size_t pos, Range &range) {
    if (pos >= _ranges.size()) {
        return false;
    } else {
        range = _ranges[pos];
        return true;
    }
}

vector<Range> RangeUtil::splitRange(uint32_t partitionCount, uint32_t rangeFrom,
                                    uint32_t rangeTo) {
    assert(rangeTo <= 65535);

    vector<Range> ranges;
    if (partitionCount == 0 || rangeFrom > rangeTo) {
        return ranges;
    }

    uint32_t rangeCount = rangeTo - rangeFrom + 1;

    uint32_t c = rangeCount / partitionCount;
    uint32_t m = rangeCount % partitionCount;

    uint32_t from = rangeFrom;
    for (uint32_t i = 0; i < partitionCount && from <= rangeTo; ++i) {
        uint32_t to = from + c + (i >= m ? 0 : 1) - 1;
        Range range(from, to);
        ranges.push_back(range);
        from = to + 1;
    }
    return ranges;
}

int32_t RangeUtil::getPartId(uint32_t hashId) {
    for (size_t i = 0; i < _ranges.size(); ++i) {
        uint32_t from = _ranges[i]._from;
        uint32_t to = _ranges[i]._to;
        if (from <= hashId && hashId <= to) {
            return i;
        }
    }
    return -1;
}

} // namespace multi_call
