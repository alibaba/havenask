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
#include "swift/broker/storage/InnerPartMetric.h"

#include <cassert>

namespace swift {
namespace storage {

// only contain 5 minutes points, one interval a point
const uint32_t TOTAL_TIME_SECONDS = 300;
InnerPartMetric::InnerPartMetric(uint32_t intervalSec) {
    _curPos = 0;
    _intervalSec = intervalSec;
    _elemCount = TOTAL_TIME_SECONDS / intervalSec + 1;
    _metricVec = new InnerPartMetricStat[_elemCount];
}

InnerPartMetric::~InnerPartMetric() {
    delete[] _metricVec;
    _metricVec = nullptr;
    _curPos = 0;
}

void InnerPartMetric::update(const InnerPartMetricStat &metric) {
    assert(_curPos < _elemCount);
    _metricVec[_curPos++] = metric;
    _curPos = (_curPos + _elemCount) % _elemCount;
}

void InnerPartMetric::getMetricDiff(uint32_t seconds, InnerPartMetricStat &metric) const {
    uint32_t current = (_curPos + _elemCount - 1) % _elemCount;
    uint32_t last = (current + _elemCount - seconds / _intervalSec) % _elemCount;
    calcDiff(current, last, metric);
}

void InnerPartMetric::getMetric1min(InnerPartMetricStat &metric) const { getMetricDiff(60, metric); }

void InnerPartMetric::getMetric5min(InnerPartMetricStat &metric) const { getMetricDiff(300, metric); }

void InnerPartMetric::calcDiff(uint32_t current, uint32_t last, InnerPartMetricStat &metric) const {
    metric.writeSize = _metricVec[current].writeSize - _metricVec[last].writeSize;
    metric.readSize = _metricVec[current].readSize - _metricVec[last].readSize;
    metric.writeRequestNum = _metricVec[current].writeRequestNum - _metricVec[last].writeRequestNum;
    metric.readRequestNum = _metricVec[current].readRequestNum - _metricVec[last].readRequestNum;
}

} // namespace storage
} // namespace swift
