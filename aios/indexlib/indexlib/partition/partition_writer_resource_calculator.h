#ifndef __INDEXLIB_PARTITION_WRITER_RESOURCE_CALCULATOR_H
#define __INDEXLIB_PARTITION_WRITER_RESOURCE_CALCULATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/operation_queue/operation_writer.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);

IE_NAMESPACE_BEGIN(partition);

class PartitionWriterResourceCalculator
{
public:
    PartitionWriterResourceCalculator(const config::IndexPartitionOptions& options);
    
    ~PartitionWriterResourceCalculator() {}
    
public:
    void Init(const index_base::SegmentWriterPtr& segWriter,
              const PartitionModifierPtr& modifier,
              const OperationWriterPtr& operationWriter);
    int64_t GetCurrentMemoryUse() const; 
    int64_t EstimateMaxMemoryUseOfCurrentSegment() const;
    int64_t EstimateDumpMemoryUse() const;
    int64_t EstimateDumpFileSize() const;

private:
    const config::IndexPartitionOptions& mOptions;
    util::BuildResourceMetricsPtr mSegWriterMetrics;
    util::BuildResourceMetricsPtr mModifierMetrics;
    util::BuildResourceMetricsPtr mOperationWriterMetrics;
    
private:
    friend class PartitionWriterResourceCalculatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionWriterResourceCalculator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_WRITER_RESOURCE_CALCULATOR_H
