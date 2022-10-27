#ifndef __INDEXLIB_CUSTOM_PARTITION_DATA_H
#define __INDEXLIB_CUSTOM_PARTITION_DATA_H

#include <tr1/memory>
#include <vector>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/segment_info.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, PartitionInfo)
DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);
DECLARE_REFERENCE_CLASS(index_base, SegmentDataBase);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, IndexFormatVersion);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentQueue);
DECLARE_REFERENCE_CLASS(table, BuildingSegmentReader)

IE_NAMESPACE_BEGIN(partition);

struct NewSegmentMeta
{
    NewSegmentMeta(const index_base::SegmentDataBasePtr& _segData,
                   const index_base::SegmentInfo& _segInfo)
    : segmentDataBase(_segData)
    , segmentInfo(_segInfo) {}
    ~NewSegmentMeta() {}
    index_base::SegmentDataBasePtr segmentDataBase;
    index_base::SegmentInfo segmentInfo;
};

DEFINE_SHARED_PTR(NewSegmentMeta);

class CustomPartitionData : public index_base::PartitionData
{
public:
    CustomPartitionData(const config::IndexPartitionOptions& options,
                        const config::IndexPartitionSchemaPtr& schema,
                        const util::PartitionMemoryQuotaControllerPtr& memController,
                        const util::BlockMemoryQuotaControllerPtr& tableWriterMemController,
                        misc::MetricProviderPtr metricProvider,
                        const util::CounterMapPtr& counterMap,
                        const plugin::PluginManagerPtr& pluginManager,
                        const DumpSegmentQueuePtr& dumpSegmentQueue,
                        int64_t reclaimTimestamp = -1);

    CustomPartitionData(const CustomPartitionData& other);
    virtual ~CustomPartitionData();
public:
    bool Open(const index_base::SegmentDirectoryPtr& segDir); 
    index_base::Version GetVersion() const override;
    index_base::Version GetOnDiskVersion() const override;
    index_base::PartitionMeta GetPartitionMeta() const override;
    const file_system::DirectoryPtr& GetRootDirectory() const override;
    const index_base::IndexFormatVersion& GetIndexFormatVersion() const override;
    Iterator Begin() const override;
    Iterator End() const override;
    index_base::SegmentData GetSegmentData(segmentid_t segId) const override;
    SegmentDataVector GetSegmentDatas() const;
    index::DeletionMapReaderPtr GetDeletionMapReader() const override
    {
        assert(false);
        return index::DeletionMapReaderPtr();
    }
    index::PartitionInfoPtr GetPartitionInfo() const override
    {
        assert(false);
        return index::PartitionInfoPtr();
    }
    CustomPartitionData* Clone() override;
    index_base::PartitionDataPtr GetSubPartitionData() const override
    {
        assert(false);
        return index_base::PartitionDataPtr();
    }
    const index_base::SegmentDirectoryPtr& GetSegmentDirectory() const override;
    uint32_t GetIndexShardingColumnNum(
        const config::IndexPartitionOptions& options) const override
    {
        assert(false);
        return 1u;
    }
    const plugin::PluginManagerPtr& GetPluginManager() const override
    {
        return mPluginManager;
    }
    index_base::PartitionSegmentIteratorPtr CreateSegmentIterator() override;

    NewSegmentMetaPtr CreateNewSegmentData();
    void AddBuiltSegment(segmentid_t segId, const index_base::SegmentInfoPtr& segInfo) override;
    void AddBuiltSegment(segmentid_t segId, const index_base::SegmentInfo& segInfo);
    std::vector<table::BuildingSegmentReaderPtr> GetDumpingSegmentReaders();

    // will trigger commit version
    void RemoveObsoleteSegments();
    void CommitVersion() override;

    util::PartitionMemoryQuotaControllerPtr GetPartitionMemoryQuotaController() const;
    util::BlockMemoryQuotaControllerPtr GetTableWriterMemoryQuotaController() const;
    std::string GetLastLocator() const override;
    const DumpSegmentQueuePtr& GetDumpSegmentQueue() const { return mDumpSegmentQueue; }
    void RemoveSegments(const std::vector<segmentid_t>& segIds); 

private:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    util::PartitionMemoryQuotaControllerPtr mMemController;
    util::BlockMemoryQuotaControllerPtr mTableWriterMemController;
    misc::MetricProviderPtr mMetricProvider;
    util::CounterMapPtr mCounterMap; 
    plugin::PluginManagerPtr mPluginManager;
    DumpSegmentQueuePtr mDumpSegmentQueue;    
    int64_t mReclaimTimestamp;
    index_base::PartitionMeta mPartitionMeta; 
    index_base::SegmentDirectoryPtr mSegmentDirectory;
    std::vector<segmentid_t> mToReclaimRtSegIds;
    NewSegmentMetaPtr mNewSegmentMetaPtr;
    mutable autil::RecursiveThreadMutex mLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomPartitionData);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_CUSTOM_PARTITION_DATA_H
