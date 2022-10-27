#ifndef __INDEXLIB_BUILDING_PARTITION_DATA_H
#define __INDEXLIB_BUILDING_PARTITION_DATA_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/misc/metric_provider.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, PartitionInfo);
DECLARE_REFERENCE_CLASS(partition, InMemorySegmentCreator);
DECLARE_REFERENCE_CLASS(partition, InMemoryPartitionData);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);

IE_NAMESPACE_BEGIN(partition);

class BuildingPartitionData : public index_base::PartitionData
{
public:
    BuildingPartitionData(const config::IndexPartitionOptions& options,
                          const config::IndexPartitionSchemaPtr& schema,
                          const util::PartitionMemoryQuotaControllerPtr& memController,
                          const DumpSegmentContainerPtr& container,
                          misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr(),
                          const util::CounterMapPtr& counterMap = util::CounterMapPtr(),
                          const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr()); 
    BuildingPartitionData(const BuildingPartitionData& other);
    virtual ~BuildingPartitionData();

public:
    void Open(const index_base::SegmentDirectoryPtr& segDir);


    BuildingPartitionData* Clone() override;
    index_base::Version GetVersion() const override;
    index_base::Version GetOnDiskVersion() const override;
    index_base::PartitionMeta GetPartitionMeta() const override;
    const index_base::IndexFormatVersion& GetIndexFormatVersion() const override;

    const file_system::DirectoryPtr& GetRootDirectory() const override;

    index_base::InMemorySegmentPtr GetInMemorySegment() const override;
    void ResetInMemorySegment() override;
    index::PartitionInfoPtr GetPartitionInfo() const override;
    void UpdatePartitionInfo() override;

    // not thread safe when segmentDatas change
    Iterator Begin() const override;
    Iterator End() const override;

    index_base::SegmentData GetSegmentData(segmentid_t segId) const override;

    void CommitVersion() override;

    void RemoveSegments(const std::vector<segmentid_t>& segmentIds) override;
    index_base::InMemorySegmentPtr CreateNewSegment() override;
    index_base::InMemorySegmentPtr CreateJoinSegment() override;

    index::DeletionMapReaderPtr GetDeletionMapReader() const override;

    // only used for reader
    index_base::PartitionDataPtr GetSubPartitionData() const override;

    const index_base::SegmentDirectoryPtr& GetSegmentDirectory() const override;

    uint32_t GetIndexShardingColumnNum(
        const config::IndexPartitionOptions& options) const override;

    segmentid_t GetLastValidRtSegmentInLinkDirectory() const override;
    bool SwitchToLinkDirectoryForRtSegments(
            segmentid_t lastLinkRtSegId) override;

    std::string GetLastLocator() const override;
    void AddBuiltSegment(segmentid_t segId, const index_base::SegmentInfoPtr& segInfo) override;
    index_base::PartitionSegmentIteratorPtr CreateSegmentIterator() override;
    const util::CounterMapPtr& GetCounterMap() const override;

    const plugin::PluginManagerPtr& GetPluginManager() const override;

public:
    void InheritInMemorySegment(const index_base::InMemorySegmentPtr& inMemSegment);

private:
    void FillIndexSizeCounters(const util::CounterMapPtr& counterMap,
                               const index_base::SegmentDirectoryPtr& segDir,
                               const config::IndexPartitionSchemaPtr& schema);
    
    index_base::InMemorySegmentPtr CreateJoinSegment(
            const InMemoryPartitionDataPtr& partitionData) const;

    void CheckSegmentDirectory(const index_base::SegmentDirectoryPtr& segDir,
                               const config::IndexPartitionOptions& options);

    uint32_t GetShardingColumnNum() const
    { return GetIndexShardingColumnNum(mOptions); }
    
protected:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    DumpSegmentContainerPtr mDumpSegmentContainer;
    InMemoryPartitionDataPtr mInMemPartitionData;

    index_base::InMemorySegmentPtr mInMemSegment;
    index_base::InMemorySegmentPtr mJoinInMemSegment;
    InMemorySegmentCreatorPtr mInMemorySegmentCreator;
    util::PartitionMemoryQuotaControllerPtr mMemController;

    mutable autil::RecursiveThreadMutex mLock;
    
private:
    friend class BuildingPartitionDataTest;
    friend class InMemoryPartitionDataTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingPartitionData);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_BUILDING_PARTITION_DATA_H
