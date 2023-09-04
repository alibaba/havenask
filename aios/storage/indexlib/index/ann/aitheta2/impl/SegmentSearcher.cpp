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

#include "indexlib/index/ann/aitheta2/impl/SegmentSearcher.h"

namespace indexlibv2::index::ann {

bool SegmentSearcher::Search(const AithetaQueries& query, const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                             ResultHolder& resultHolder)
{
    ScopedLatencyReporter reporter(_seekLatencyMetric);

    resultHolder.ResetBaseDocId(_segmentBaseDocId);
    size_t priorResultCount = resultHolder.GetResultSize();
    const auto priorResultStats = resultHolder.GetResultStats();

    ANN_CHECK(DoSearch(query, searchInfo, resultHolder), "segment search failed");

    size_t resultCount = resultHolder.GetResultSize() - priorResultCount;
    METRIC_REPORT(_resultCountMetric, resultCount);
    size_t filteredCount = resultHolder.GetResultStats().filteredCount - priorResultStats.filteredCount;
    METRIC_REPORT(_filteredCountMetric, filteredCount);
    size_t distCalcCount = resultHolder.GetResultStats().distCalcCount - priorResultStats.distCalcCount;
    METRIC_REPORT(_distCalcCountMetric, distCalcCount);

    return true;
}

void SegmentSearcher::InitSearchMetrics()
{
    METRIC_SETUP(_resultCountMetric, "indexlib.vector.segment_result_count", kmonitor::GAUGE);
    METRIC_SETUP(_seekLatencyMetric, "indexlib.vector.segment_seek_latency", kmonitor::GAUGE);
    METRIC_SETUP(_filteredCountMetric, "indexlib.vector.segment_filtered_count", kmonitor::GAUGE);
    METRIC_SETUP(_distCalcCountMetric, "indexlib.vector.segment_dist_calc_count", kmonitor::GAUGE);
}

AUTIL_LOG_SETUP(indexlib.index, SegmentSearcher);

} // namespace indexlibv2::index::ann
