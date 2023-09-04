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
#include "indexlib/table/common/TabletWriterResourceCalculator.h"

#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib::table {
AUTIL_LOG_SETUP(indexlib.table, TabletWriterResourceCalculator);

TabletWriterResourceCalculator::TabletWriterResourceCalculator(
    const std::shared_ptr<util::BuildResourceMetrics>& buildingSegmentMetrics, bool isOnline, int64_t dumpThreadCount)
    : _isOnline(isOnline)
    , _dumpThreadCount(dumpThreadCount)
    , _buildingSegmentMetrics(buildingSegmentMetrics)
{
}

int64_t TabletWriterResourceCalculator::GetCurrentMemoryUse() const
{
    int64_t currentMemUse = 0;
    currentMemUse +=
        (_buildingSegmentMetrics ? _buildingSegmentMetrics->GetValue(indexlib::util::BMT_CURRENT_MEMORY_USE) : 0);
    return currentMemUse;
}

int64_t TabletWriterResourceCalculator::EstimateMaxMemoryUseOfCurrentSegment() const
{
    auto currentMemoryUse = GetCurrentMemoryUse();
    auto dumpMemoryUse = EstimateDumpMemoryUse();
    auto dumpFileSize = (!_isOnline ? EstimateDumpFileSize() : 0);
    auto maxMemoryUse = currentMemoryUse + dumpMemoryUse + dumpFileSize;
    AUTIL_LOG(DEBUG, "currentMemoryUse[%ld] + dumpMemoryUse[%ld] + dumpFileSize[%ld] = maxMemoryUse[%ld]",
              currentMemoryUse, dumpMemoryUse, dumpFileSize, maxMemoryUse);
    return maxMemoryUse;
}

int64_t TabletWriterResourceCalculator::EstimateDumpMemoryUse() const
{
    int64_t dumpExpandMemUse = 0;
    int64_t maxDumpTempMemUse = 0;
    int64_t nodeCount = 0;
    if (_buildingSegmentMetrics) {
        dumpExpandMemUse += _buildingSegmentMetrics->GetValue(indexlib::util::BMT_DUMP_EXPAND_MEMORY_SIZE);
        maxDumpTempMemUse =
            std::max(maxDumpTempMemUse, _buildingSegmentMetrics->GetValue(indexlib::util::BMT_DUMP_TEMP_MEMORY_SIZE));
        nodeCount += _buildingSegmentMetrics->GetNodeCount();
    }

    // operation writer and segmentWriter use multi thread dump queue
    if (nodeCount < _dumpThreadCount) {
        maxDumpTempMemUse = nodeCount * maxDumpTempMemUse;
    } else {
        maxDumpTempMemUse = _dumpThreadCount * maxDumpTempMemUse;
    }
    return dumpExpandMemUse + maxDumpTempMemUse;
}

int64_t TabletWriterResourceCalculator::EstimateDumpFileSize() const
{
    int64_t dumpFileSize = 0;
    dumpFileSize +=
        (_buildingSegmentMetrics ? _buildingSegmentMetrics->GetValue(indexlib::util::BMT_DUMP_FILE_SIZE) : 0);
    return dumpFileSize;
}

} // namespace indexlib::table
