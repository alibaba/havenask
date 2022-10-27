#ifndef __INDEXLIB_SPLIT_SEGMENT_STRATEGY_H
#define __INDEXLIB_SPLIT_SEGMENT_STRATEGY_H

#include <tr1/memory>
#include <array>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/segment_metrics_updater.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/util/key_value_map.h"

IE_NAMESPACE_BEGIN(merger);

class SplitSegmentStrategy
{
public:
    // static constexpr size_t MAX_SEGMENT_COUNT = 256;

public:
    SplitSegmentStrategy(index::SegmentDirectoryBasePtr segDir,
                         config::IndexPartitionSchemaPtr schema, 
                         index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
                         const index_base::SegmentMergeInfos& segMergeInfos,
                         const MergePlan& plan)                         
        : mSegDir(segDir)
        , mSchema(schema)
        , mReaderContainer(attrReaders)
        , mSegMergeInfos(segMergeInfos)
        , mPlan(plan)
    {
    }

    virtual ~SplitSegmentStrategy() {}

public:
    virtual bool Init(const util::KeyValueMap& parameters) = 0;

public:
    void SetAttrUpdaterGenerator(std::function<std::unique_ptr<index::SegmentMetricsUpdater>()> fn)
    {
        mUpdaterGenerator = std::move(fn);
    }
    segmentindex_t Process(segmentid_t oldSegId, docid_t oldLocalId);
    std::vector<autil::legacy::json::JsonMap> GetSegmentCustomizeMetrics();

protected:
    virtual segmentindex_t DoProcess(segmentid_t oldSegId, docid_t oldLocalId) = 0;

protected:
    index::SegmentDirectoryBasePtr mSegDir;
    config::IndexPartitionSchemaPtr mSchema;
    index::OfflineAttributeSegmentReaderContainerPtr mReaderContainer;
    index_base::SegmentMergeInfos mSegMergeInfos;
    MergePlan mPlan;
    std::function<std::unique_ptr<index::SegmentMetricsUpdater>()> mUpdaterGenerator;
    std::vector<std::unique_ptr<index::SegmentMetricsUpdater>> mMetricsUpdaters;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SplitSegmentStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_SPLIT_SEGMENT_STRATEGY_H
