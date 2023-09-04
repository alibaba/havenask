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
#ifndef ISEARCH_MULTI_CALL_RANGEUTIL_H
#define ISEARCH_MULTI_CALL_RANGEUTIL_H

#include "autil/Log.h"

namespace multi_call {
struct Range {
public:
    Range(uint32_t from, uint32_t to) : _from(from), _to(to) {
    }

public:
    uint32_t _from;
    uint32_t _to;
};

class RangeUtil
{
public:
    RangeUtil(uint32_t partCount, uint32_t rangeFrom = 0, uint32_t rangeTo = 65535);
    ~RangeUtil();

private:
    RangeUtil(const RangeUtil &);
    RangeUtil &operator=(const RangeUtil &);

public:
    static std::vector<Range> splitRange(uint32_t partitionCount, uint32_t rangeFrom = 0,
                                         uint32_t rangeTo = 65535);

public:
    const std::vector<Range> &getRanges();
    bool getRange(size_t pos, Range &range);
    int32_t getPartId(uint32_t hashId);

private:
    std::vector<Range> _ranges;
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_RANGEUTIL_H
