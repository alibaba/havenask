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
#include "indexlib/partition/partition_writer_resource_calculator.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/segment/segment_writer.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionWriterResourceCalculator);

PartitionWriterResourceCalculator::PartitionWriterResourceCalculator(const config::IndexPartitionOptions& options)
    : mIsOnline(!options.IsOffline())
    , mDumpThreadCount(options.GetBuildConfig().dumpThreadCount)
{
}

PartitionWriterResourceCalculator::PartitionWriterResourceCalculator(bool isOnline, int64_t dumpThreadCount)
    : mIsOnline(isOnline)
    , mDumpThreadCount(dumpThreadCount)
{
}

void PartitionWriterResourceCalculator::Init(const SegmentWriterPtr& segWriter,
                                             const util::BuildResourceMetricsPtr& modifierMetrics,
                                             const OperationWriterPtr& operationWriter)
{
    if (segWriter) {
        mSegWriterMetrics = segWriter->GetBuildResourceMetrics();
    }
    mModifierMetrics = modifierMetrics;
    if (operationWriter) {
        mOperationWriterMetrics = operationWriter->GetBuildResourceMetrics();
    }
}

void PartitionWriterResourceCalculator::Init(const util::BuildResourceMetricsPtr& segMetrics,
                                             const util::BuildResourceMetricsPtr& modifierMetrics,
                                             const util::BuildResourceMetricsPtr& operationMetrics)
{
    mSegWriterMetrics = segMetrics;
    mModifierMetrics = modifierMetrics;
    mOperationWriterMetrics = operationMetrics;
}

int64_t PartitionWriterResourceCalculator::GetCurrentMemoryUse() const
{
    int64_t currentMemUse = 0;
    currentMemUse += (mSegWriterMetrics ? mSegWriterMetrics->GetValue(BMT_CURRENT_MEMORY_USE) : 0);
    currentMemUse += (mOperationWriterMetrics ? mOperationWriterMetrics->GetValue(BMT_CURRENT_MEMORY_USE) : 0);
    currentMemUse += (mModifierMetrics ? mModifierMetrics->GetValue(BMT_CURRENT_MEMORY_USE) : 0);
    return currentMemUse;
}

int64_t PartitionWriterResourceCalculator::EstimateMaxMemoryUseOfCurrentSegment() const
{
    return GetCurrentMemoryUse() + EstimateDumpMemoryUse() + (!mIsOnline ? EstimateDumpFileSize() : 0);
}

int64_t PartitionWriterResourceCalculator::EstimateDumpMemoryUse() const
{
    int64_t dumpExpandMemUse = 0;
    int64_t maxDumpTempMemUse = 0;
    int64_t nodeCount = 0;
    if (mSegWriterMetrics) {
        dumpExpandMemUse += mSegWriterMetrics->GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE);
        maxDumpTempMemUse = max(maxDumpTempMemUse, mSegWriterMetrics->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
        nodeCount += mSegWriterMetrics->GetNodeCount();
    }

    if (mOperationWriterMetrics) {
        dumpExpandMemUse += mOperationWriterMetrics->GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE);
        maxDumpTempMemUse = max(maxDumpTempMemUse, mOperationWriterMetrics->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
        nodeCount += mOperationWriterMetrics->GetNodeCount();
    }
    // operation writer and segmentWriter use multi thread dump queue
    if (nodeCount < mDumpThreadCount) {
        maxDumpTempMemUse = nodeCount * maxDumpTempMemUse;
    } else {
        maxDumpTempMemUse = mDumpThreadCount * maxDumpTempMemUse;
    }
    if (mModifierMetrics) {
        dumpExpandMemUse += mModifierMetrics->GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE);
        // modifier dumps after op writer & segWriter, and use single thread dump
        maxDumpTempMemUse = max(maxDumpTempMemUse, mModifierMetrics->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
    }
    return dumpExpandMemUse + maxDumpTempMemUse;
}

int64_t PartitionWriterResourceCalculator::EstimateDumpFileSize() const
{
    int64_t dumpFileSize = 0;
    dumpFileSize += (mSegWriterMetrics ? mSegWriterMetrics->GetValue(BMT_DUMP_FILE_SIZE) : 0);
    dumpFileSize += (mOperationWriterMetrics ? mOperationWriterMetrics->GetValue(BMT_DUMP_FILE_SIZE) : 0);
    dumpFileSize += (mModifierMetrics ? mModifierMetrics->GetValue(BMT_DUMP_FILE_SIZE) : 0);
    return dumpFileSize;
}
}} // namespace indexlib::partition
