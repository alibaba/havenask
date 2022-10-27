#ifndef __INDEXLIB_IN_MEMORY_SEGMENT_CREATOR_H
#define __INDEXLIB_IN_MEMORY_SEGMENT_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(partition, SingleSegmentWriter);
DECLARE_REFERENCE_CLASS(partition, InMemoryPartitionData);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(partition);

class InMemorySegmentCreator
{
public:
    InMemorySegmentCreator(uint32_t shardingColumnNum = 1);
    ~InMemorySegmentCreator();
public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              const config::IndexPartitionOptions& options);

    index_base::InMemorySegmentPtr Create(const InMemoryPartitionDataPtr& inMemPartData,
            const util::PartitionMemoryQuotaControllerPtr& memController);

    index_base::InMemorySegmentPtr Create(const InMemoryPartitionDataPtr& inMemPartData,
            const index_base::InMemorySegmentPtr& inMemorySegment);

private:
    index_base::InMemorySegmentPtr Create(
            const index_base::PartitionSegmentIteratorPtr& partitionSegIter,
            const index_base::BuildingSegmentData& segmentData,
            const file_system::DirectoryPtr& segParentDirectory,
            const util::PartitionMemoryQuotaControllerPtr& memController,
            const util::CounterMapPtr& counterMap,
            const plugin::PluginManagerPtr& pluginManager);

    index_base::InMemorySegmentPtr Create(const index_base::BuildingSegmentData& segmentData,
                              const file_system::DirectoryPtr& segParentDirectory,
                              const index_base::InMemorySegmentPtr& inMemorySegment);

    index_base::SegmentWriterPtr DoCreate(
            const index_base::PartitionSegmentIteratorPtr& partitionSegIter,
            index_base::BuildingSegmentData& segmentData,
            const index_base::SegmentMetricsPtr& metrics,
            const util::BlockMemoryQuotaControllerPtr& segmentMemController,
            const util::CounterMapPtr& counterMap,
            const plugin::PluginManagerPtr& pluginManager);

    SingleSegmentWriterPtr CreateSingleSegmentWriter(
            const index_base::BuildingSegmentData& segmentData,
            const index_base::SegmentMetricsPtr& metrics,
            const util::BlockMemoryQuotaControllerPtr& segmentMemController,
            const util::CounterMapPtr& counterMap,
            const plugin::PluginManagerPtr& pluginManager,
            const index_base::PartitionSegmentIteratorPtr& partitionSegIter);

    index_base::InMemorySegmentPtr CreateInMemorySegment(
            const index_base::BuildingSegmentData& segmentData,
            const index_base::SegmentWriterPtr& segmentWriter,
            const config::BuildConfig& buildConfig,
            bool isSubSegment,
            const util::BlockMemoryQuotaControllerPtr& segmentMemController,
            const util::CounterMapPtr& counterMap);

private:
    void UpdateSegmentMetrics(const index_base::PartitionDataPtr& partData);

    void SetSegmentInitMemUseRatio(
            const index_base::SegmentMetricsPtr& metrics, size_t initMem,
            const util::BlockMemoryQuotaControllerPtr& segmentMemController);

private:
    static void ExtractNonTruncateIndexNames(
            const config::IndexPartitionSchemaPtr& schema,
            std::vector<std::string>& indexNames);

    static void ExtractNonTruncateIndexNames(
            const config::IndexSchemaPtr& indexSchemaPtr,
            std::vector<std::string>& indexNames);

private:
    index_base::SegmentMetricsPtr mSegmentMetrics;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    uint32_t mColumnNum;

private:
    friend class InMemorySegmentTest;
    friend class InMemorySegmentCreatorTest;
    friend class BuildingPartitionDataTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemorySegmentCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_IN_MEMORY_SEGMENT_CREATOR_H
