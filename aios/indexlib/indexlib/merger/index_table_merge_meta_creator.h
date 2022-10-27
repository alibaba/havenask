#ifndef __INDEXLIB_INDEX_TABLE_MERGE_META_CREATOR_H
#define __INDEXLIB_INDEX_TABLE_MERGE_META_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/merge_meta_creator.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/merger/merge_plan_meta.h"
#include "indexlib/merger/dump_strategy.h"
#include "indexlib/merger/segment_directory.h"

DECLARE_REFERENCE_CLASS(merger, SplitSegmentStrategyFactory);

IE_NAMESPACE_BEGIN(merger);

// TODO: refactor

class IndexTableMergeMetaCreator : public MergeMetaCreator
{
public:
    static const size_t MAX_THREAD_COUNT;
public:
    IndexTableMergeMetaCreator(const config::IndexPartitionSchemaPtr& schema,
                               const index::ReclaimMapCreatorPtr& reclaimMapCreator,
                               SplitSegmentStrategyFactory* splitStrategyFactory);
    ~IndexTableMergeMetaCreator();
public:
    void Init(const merger::SegmentDirectoryPtr& segmentDirectory,
              const config::MergeConfig& mergeConfig,
              const merger::DumpStrategyPtr& dumpStrategy,
              const plugin::PluginManagerPtr& pluginManager,
              uint32_t instanceCount) override final;

    IndexMergeMeta* Create(MergeTask &task) override final;

private:
    void CheckMergeTask(const MergeTask& task) const;
    void CreateMergeTaskForIncTruncate(MergeTask& task);
    void GetNotMergedSegments(index_base::SegmentMergeInfos& segments);
    void CreateInPlanMergeInfos(
        index_base::SegmentMergeInfos& inPlanSegMergeInfos,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const merger::MergePlan& plan) const;
    void CreateNotInPlanMergeInfos(
        index_base::SegmentMergeInfos& notInPlanSegMergeInfos,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const MergeTask &task) const;
    index_base::Version CreateNewVersion(IndexMergeMeta& meta, MergeTask& task);
    void SchedulerMergeTaskItem(IndexMergeMeta& meta, uint32_t instanceCount);
    std::string GetMergeResourceRootDir() const;
    void ResetTargetSegmentId(IndexMergeMeta& meta, segmentid_t lastSegmentId);

private:
    config::IndexPartitionSchemaPtr mSchema;
    merger::SegmentDirectoryPtr mSegmentDirectory;
    config::MergeConfig mMergeConfig;
    index_base::SegmentMergeInfos mSegMergeInfos;
    merger::DumpStrategyPtr mDumpStrategy;
    index::ReclaimMapCreatorPtr mReclaimMapCreator;
    SplitSegmentStrategyFactory* mSplitStrategyFactory;
    plugin::PluginManagerPtr mPluginManager;
    uint32_t mInstanceCount;

private:
    friend class IndexTableMergeMetaCreatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexTableMergeMetaCreator);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INDEX_TABLE_MERGE_META_CREATOR_H
