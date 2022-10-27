#ifndef __INDEXLIB_IN_MEMORY_PARTITION_DATA_H
#define __INDEXLIB_IN_MEMORY_PARTITION_DATA_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/misc/metric_provider.h"
#include "indexlib/misc/monitor.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, StateCounter);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);

IE_NAMESPACE_BEGIN(partition);

class InMemoryPartitionData;
DEFINE_SHARED_PTR(InMemoryPartitionData);

class InMemoryPartitionData : public OnDiskPartitionData
{
public:
    InMemoryPartitionData(
        const DumpSegmentContainerPtr& container,
        misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr(),
        bool needDeletionMap = true,
        const util::CounterMapPtr& counterMap = util::CounterMapPtr(),
        const config::IndexPartitionOptions& options =  config::IndexPartitionOptions(),
        const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    ~InMemoryPartitionData();
public:
    void Open(const index_base::SegmentDirectoryPtr& segDir,
              bool needDeletionMap = false) override;

    void RemoveSegments(const std::vector<segmentid_t>& segIds) override;
    InMemoryPartitionData* Clone() override;
    void UpdatePartitionInfo() override;
    index_base::InMemorySegmentPtr GetInMemorySegment() const override
    { return mInMemSegment; }
    void CommitVersion() override;
    index_base::PartitionSegmentIteratorPtr CreateSegmentIterator() override;
    void ResetInMemorySegment() override
    { mInMemSegment.reset(); }

    void AddSegment(segmentid_t segId, timestamp_t ts);
    void AddSegmentAndUpdateData(segmentid_t segId, timestamp_t ts);
    index_base::BuildingSegmentData CreateNewSegmentData();
    void SetInMemorySegment(const index_base::InMemorySegmentPtr& inMemSegment);
    const util::CounterMapPtr& GetCounterMap() const override { return mCounterMap; }
    void SetDumpSegmentContainer(const DumpSegmentContainerPtr& container);
    
private:
    void MergeCounterMap(const index_base::SegmentDirectoryPtr& segDir);
    void UpdateData();
    void ReportPartitionDocCount();
    void ReportDelDocCount();
    void ReportSegmentCount();

    void RegisterMetrics();
    void InitCounters(const util::CounterMapPtr& counterMap); 

    InMemoryPartitionData* GetSubInMemoryPartitionData() const;
    bool IsSubPartitionData() const;
    PartitionInfoHolderPtr CreatePartitionInfoHolder() const override;
private:
    index_base::InMemorySegmentPtr mInMemSegment;
    misc::MetricProviderPtr mMetricProvider;
    bool mNeedDeletionMap;
    util::CounterMapPtr mCounterMap;
    config::IndexPartitionOptions mOptions;
    
    IE_DECLARE_METRIC(partitionDocCount);
    IE_DECLARE_METRIC(segmentCount);
    IE_DECLARE_METRIC(incSegmentCount);
    IE_DECLARE_METRIC(deletedDocCount);

    util::StateCounterPtr mPartitionDocCounter;
    util::StateCounterPtr mDeletedDocCounter;
    DumpSegmentContainerPtr mDumpSegmentContainer;
    
private:
    friend class PartitionCounterTest;
    friend class BuildingPartitionDataTest;
    friend class InMemoryPartitionDataTest;
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_IN_MEMORY_PARTITION_DATA_H
