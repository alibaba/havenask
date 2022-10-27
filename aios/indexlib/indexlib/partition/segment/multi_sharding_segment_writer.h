#ifndef __INDEXLIB_MULTI_SHARDING_SEGMENT_WRITER_H
#define __INDEXLIB_MULTI_SHARDING_SEGMENT_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/building_segment_data.h"

DECLARE_REFERENCE_CLASS(index, ShardingPartitioner);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);

IE_NAMESPACE_BEGIN(partition);

class MultiShardingSegmentWriter : public index_base::SegmentWriter
{
public:
    MultiShardingSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                               const config::IndexPartitionOptions& options,
                               const std::vector<index_base::SegmentWriterPtr>& shardingWriters);

    ~MultiShardingSegmentWriter();

public:
    void Init(index_base::BuildingSegmentData& segmentData,
              const index_base::SegmentMetricsPtr& metrics,
              const util::QuotaControlPtr& buildMemoryQuotaControler,
              const util::BuildResourceMetricsPtr& buildResourceMetrics) override;

    MultiShardingSegmentWriter* CloneWithNewSegmentData(
        index_base::BuildingSegmentData& segmentData) const override;
    bool AddDocument(const document::DocumentPtr& doc) override;
    bool IsDirty() const override;
    bool NeedForceDump() override;
    void EndSegment() override;
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<common::DumpItem*>& dumpItems) override;
    void CollectSegmentMetrics() override;
    index::InMemorySegmentReaderPtr CreateInMemSegmentReader() override;

    const index_base::SegmentInfoPtr& GetSegmentInfo() const override;
    InMemorySegmentModifierPtr GetInMemorySegmentModifier() override;

    size_t EstimateInitMemUse(
            const index_base::SegmentMetricsPtr& metrics,
            const util::QuotaControlPtr& buildMemoryQuotaControler,
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr(),
            const index_base::PartitionSegmentIteratorPtr& partSegIter =
            index_base::PartitionSegmentIteratorPtr()) override
    {
        size_t memSize = 0;
        for (size_t i = 0; i < mShardingWriters.size(); i++)
        {
            memSize += mShardingWriters[i]->EstimateInitMemUse(metrics,
                    buildMemoryQuotaControler, pluginManager, partSegIter);
        }
        return memSize;
    }

private:
    MultiShardingSegmentWriter(const MultiShardingSegmentWriter& other,
                               index_base::BuildingSegmentData& segmentData);

private:
    index::ShardingPartitionerPtr mPartitioner;
    std::vector<index_base::SegmentWriterPtr> mShardingWriters;
    index_base::SegmentInfoPtr mCurrentSegmentInfo;
    index_base::BuildingSegmentData mSegmentData;
    bool mNeedForceDump;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiShardingSegmentWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MULTI_SHARDING_SEGMENT_WRITER_H
