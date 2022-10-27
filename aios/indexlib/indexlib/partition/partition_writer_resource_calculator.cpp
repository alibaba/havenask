#include "indexlib/partition/partition_writer_resource_calculator.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/config/index_partition_options.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionWriterResourceCalculator);

PartitionWriterResourceCalculator::PartitionWriterResourceCalculator(
    const config::IndexPartitionOptions& options)
    : mOptions(options)
{}

void PartitionWriterResourceCalculator::Init(const SegmentWriterPtr& segWriter,
                                             const PartitionModifierPtr& modifier,
                                             const OperationWriterPtr& operationWriter)
{
    if (segWriter)
    {
        mSegWriterMetrics = segWriter->GetBuildResourceMetrics();
    }
    if (modifier)
    {
        mModifierMetrics = modifier->GetBuildResourceMetrics();
    }
    if (operationWriter)
    {
        mOperationWriterMetrics = operationWriter->GetBuildResourceMetrics();
    }        
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
    return GetCurrentMemoryUse() + EstimateDumpMemoryUse() +
        (mOptions.IsOffline() ? EstimateDumpFileSize() : 0);
}

int64_t PartitionWriterResourceCalculator::EstimateDumpMemoryUse() const
{
    int64_t dumpExpandMemUse = 0;
    int64_t maxDumpTempMemUse = 0;
    int64_t dumpThreadCount = mOptions.GetBuildConfig().dumpThreadCount;
    if (mSegWriterMetrics)
    {
        dumpExpandMemUse += mSegWriterMetrics->GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE);
        maxDumpTempMemUse = max(maxDumpTempMemUse, mSegWriterMetrics->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
    }

    if (mOperationWriterMetrics)
    {
        dumpExpandMemUse += mOperationWriterMetrics->GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE);
        maxDumpTempMemUse = max(maxDumpTempMemUse, mOperationWriterMetrics->GetValue(
                        BMT_DUMP_TEMP_MEMORY_SIZE));
    }

    // operation writer and segmentWriter use multi thread dump queue
    maxDumpTempMemUse = dumpThreadCount * maxDumpTempMemUse;
    
    if (mModifierMetrics)
    {
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

IE_NAMESPACE_END(partition);

