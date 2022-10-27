#ifndef __INDEXLIB_ON_DISK_REALTIME_DATA_CALCULATOR_H
#define __INDEXLIB_ON_DISK_REALTIME_DATA_CALCULATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, SegmentLockSizeCalculator);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

IE_NAMESPACE_BEGIN(partition);

class OnDiskRealtimeDataCalculator
{
public:
    OnDiskRealtimeDataCalculator();
    ~OnDiskRealtimeDataCalculator();
    
public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              const index_base::PartitionDataPtr& partData,
              const plugin::PluginManagerPtr& pluginManager);
    
    size_t CalculateExpandSize(
            segmentid_t lastRtSegmentId, segmentid_t currentRtSegId);
    
    size_t CalculateLockSize(segmentid_t rtSegId);

private:
    index_base::PartitionDataPtr mPartitionData;
    index::SegmentLockSizeCalculatorPtr mCalculator;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskRealtimeDataCalculator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ON_DISK_REALTIME_DATA_CALCULATOR_H
