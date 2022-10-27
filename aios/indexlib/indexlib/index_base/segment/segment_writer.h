#ifndef __INDEXLIB_COMMON_SEGMENT_SEGMENT_WRITER_H
#define __INDEXLIB_COMMON_SEGMENT_SEGMENT_WRITER_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/status.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/util/memory_control/build_resource_calculator.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index, InMemorySegmentReader);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_STRUCT(index_base, BuildingSegmentData);
DECLARE_REFERENCE_STRUCT(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(partition, InMemorySegmentModifier);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, QuotaControl);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_STRUCT(common, DumpItem);
DECLARE_REFERENCE_STRUCT(document, Document);
DECLARE_REFERENCE_STRUCT(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(index_base);

class SegmentWriter
{
public:
    SegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                  const config::IndexPartitionOptions& options,
                  uint32_t columnIdx = 0, uint32_t totalColumnCount = 1)
        : mSchema(schema)
        , mOptions(options)
        , mColumnIdx(columnIdx)
        , mTotalColumnCount(totalColumnCount)
        , mStatus(misc::UNKNOWN)
    {}
    
    virtual ~SegmentWriter() {}

public:
    // for kv & kkv init
    virtual void Init(index_base::BuildingSegmentData& segmentData,
              const index_base::SegmentMetricsPtr& metrics,
              const util::QuotaControlPtr& buildMemoryQuotaControler,
              const util::BuildResourceMetricsPtr& buildResourceMetrics
              = util::BuildResourceMetricsPtr())
    {
        assert(false);
    }

    virtual void CollectSegmentMetrics() = 0;
    // for index table
    virtual void Init(const index_base::SegmentData& segmentData,
                      const index_base::SegmentMetricsPtr& metrics,
                      const util::BuildResourceMetricsPtr& buildResourceMetrics,
                      const util::CounterMapPtr& counterMap,
                      const plugin::PluginManagerPtr& pluginManager,
                      const index_base::PartitionSegmentIteratorPtr& partSegIter =
                      index_base::PartitionSegmentIteratorPtr())
    {
        assert(false);
    }

    virtual SegmentWriter* CloneWithNewSegmentData(
        BuildingSegmentData& segmentData) const = 0;

    virtual size_t EstimateInitMemUse(
            const index_base::SegmentMetricsPtr& segmentMetrics,
            const util::QuotaControlPtr& buildMemoryQuotaControler,
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr(),
            const index_base::PartitionSegmentIteratorPtr& partSegIter =
            index_base::PartitionSegmentIteratorPtr()) = 0;

    virtual bool AddDocument(const document::DocumentPtr& doc) = 0;
    virtual bool IsDirty() const = 0;
    virtual void EndSegment() = 0;
    virtual void CreateDumpItems(const file_system::DirectoryPtr& directory,
                                 std::vector<common::DumpItem*>& dumpItems) = 0;
    virtual index::InMemorySegmentReaderPtr CreateInMemSegmentReader() = 0;
    virtual size_t GetTotalMemoryUse() const;
    virtual const index_base::SegmentInfoPtr& GetSegmentInfo() const = 0;
    virtual partition::InMemorySegmentModifierPtr GetInMemorySegmentModifier() = 0;
    virtual bool NeedForceDump() { return false; }
    virtual misc::Status GetStatus() const { return mStatus; }
    const util::CounterMapPtr& GetCounterMap() const { return mCounterMap; }
    const util::BuildResourceMetricsPtr& GetBuildResourceMetrics() const
    { return mBuildResourceMetrics; }

    bool IsShardingColumn() const { return mTotalColumnCount > 1; }
    uint32_t GetColumnIdx() const { return mColumnIdx; }
    uint32_t GetTotalColumnCount() const { return mTotalColumnCount; }
    const index_base::SegmentMetricsPtr& GetSegmentMetrics() const { return mSegmentMetrics; }

    static std::string GetMetricsGroupName(uint32_t columnIdx)
    { return index_base::SegmentMetricsUtil::GetColumnGroupName(columnIdx); }

protected:
    void SetStatus(misc::Status status) { mStatus = status; }
    
protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    util::BuildResourceMetricsPtr mBuildResourceMetrics;
    uint32_t mColumnIdx;
    uint32_t mTotalColumnCount;
    volatile misc::Status mStatus;
    util::CounterMapPtr mCounterMap;
    index_base::SegmentMetricsPtr mSegmentMetrics;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentWriter);

///////////////////////////////////////////////////////////////////
inline size_t SegmentWriter::GetTotalMemoryUse() const
{
    assert(mBuildResourceMetrics);
    return util::BuildResourceCalculator::GetCurrentTotalMemoryUse(mBuildResourceMetrics);
}

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_COMMON_SEGMENT_SEGMENT_WRITER_H
