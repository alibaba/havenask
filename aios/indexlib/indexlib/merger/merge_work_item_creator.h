#ifndef __INDEXLIB_MERGE_WORK_ITEM_CREATOR_H
#define __INDEXLIB_MERGE_WORK_ITEM_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer_creator.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/summary/summary_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/merger/merge_work_item_typed.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/merger/index_partition_merger_metrics.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResource);
DECLARE_REFERENCE_CLASS(index, TruncateIndexWriter);
DECLARE_REFERENCE_CLASS(index, AdaptiveBitmapIndexWriter);
DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);

IE_NAMESPACE_BEGIN(merger);

class MergeWorkItemCreator
{
public:
    MergeWorkItemCreator(const config::IndexPartitionSchemaPtr &schema,
                         const config::MergeConfig &mergeConfig,
                         const IndexMergeMeta *mergeMeta,
                         const merger::SegmentDirectoryPtr &segmentDirectory,
                         const merger::SegmentDirectoryPtr &subSegmentDirectory,
                         bool isSortedMerge, bool optimize,
                         IndexPartitionMergerMetrics* metrics,
                         const util::CounterMapPtr& counterMap,
                         const config::IndexPartitionOptions& options,
                         const merger::MergeFileSystemPtr& mergeFileSystem,
                         const plugin::PluginManagerPtr& pluginManager);
    virtual ~MergeWorkItemCreator();
public:
    //virtual for test
    virtual MergeWorkItem *CreateMergeWorkItem(const MergeTaskItem &item);

private:
    virtual index::DeletionMapMergerPtr CreateDeletionMapMerger() const;

    virtual index::SummaryMergerPtr CreateSummaryMerger(
            const MergeTaskItem &item,
            const config::IndexPartitionSchemaPtr &schema) const;

    virtual index::AttributeMergerPtr CreateAttributeMerger(
        const MergeTaskItem& item, const config::IndexPartitionSchemaPtr& schema) const;

    virtual index::IndexMergerPtr CreateIndexMerger(const MergeTaskItem& item,
            const config::IndexPartitionSchemaPtr& schema,
            index::TruncateIndexWriterCreator* truncateCreator,
            index::AdaptiveBitmapIndexWriterCreator* adaptiveCreator) const;

private:
    MergeWorkItem *DoCreateMergeWorkItem(
        const MergePlan& plan,
        const MergeTaskItem &item,
        const index_base::SegmentMergeInfos &inPlanSegMergeInfos,
        const index::ReclaimMapPtr &mainReclaimMap,
        const index::ReclaimMapPtr &subReclaimMap,
        const merger::SegmentDirectoryPtr &segmentDir,
        index_base::OutputSegmentMergeInfos outputSegmentMergeInfos);

    void PrepareDirectory(index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
                          const std::string& subPath) const;

    static std::string GetGlobalResourceRoot(
            const std::string &dumpRootPath, bool isRt);
private:
    void InitCreators(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    std::vector<index_base::MergeTaskResourcePtr> ExtractMergeTaskResource(
            const MergeTaskItem &item) const;
    void PrepareOutputSegMergeInfos(const MergeTaskItem& item,
                                    const MergePlan& plan,
                                    index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::MergeConfig mMergeConfig;
    const IndexMergeMeta *mMergeMeta;
    merger::SegmentDirectoryPtr mSegmentDirectory;
    merger::SegmentDirectoryPtr mSubSegDir;
    bool mIsSortedMerge;
    bool mIsOptimize;
    std::string mDumpPath;
    std::vector<index::TruncateAttributeReaderCreator*> mTruncateAttrReaderCreators;
    std::vector<index::TruncateIndexWriterCreator*> mTruncateIndexWriterCreators;
    std::vector<index::AdaptiveBitmapIndexWriterCreator*> mAdaptiveBitmapWriterCreators;
    std::vector<index::AdaptiveBitmapIndexWriterCreator*> mSubAdaptiveBitmapWriterCreators;

    IndexPartitionMergerMetrics* mMetrics;
    util::CounterMapPtr mCounterMap;
    config::IndexPartitionOptions mOptions;

    merger::MergeFileSystemPtr mMergeFileSystem;

    plugin::PluginManagerPtr mPluginManager;
    index_base::MergeTaskResourceManagerPtr mMergeTaskResourceMgr;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeWorkItemCreator);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_WORK_ITEM_CREATOR_H
