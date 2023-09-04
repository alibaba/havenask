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

#include "swift/common/Common.h"

namespace swift {
namespace storage {

class InnerPartMetricStat {
public:
    uint64_t writeSize = 0;
    uint64_t readSize = 0;
    uint64_t writeRequestNum = 0;
    uint64_t readRequestNum = 0;
};

class InnerPartMetric {
public:
    InnerPartMetric(uint32_t intervalSec = 5);
    ~InnerPartMetric();

private:
    InnerPartMetric(const InnerPartMetric &);
    InnerPartMetric &operator=(const InnerPartMetric &);

public:
    void update(const InnerPartMetricStat &metric);
    void getMetric1min(InnerPartMetricStat &metric) const;
    void getMetric5min(InnerPartMetricStat &metric) const;

private:
    void getMetricDiff(uint32_t seconds, InnerPartMetricStat &metric) const;
    void calcDiff(uint32_t current, uint32_t last, InnerPartMetricStat &metric) const;

private:
    InnerPartMetricStat *_metricVec;
    uint32_t _intervalSec;
    uint32_t _elemCount;
    uint32_t _curPos;
};

SWIFT_TYPEDEF_PTR(InnerPartMetric);

} // namespace storage
} // namespace swift
