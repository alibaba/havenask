/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_MERGE_WORK_ITEM_CREATOR_H
#define __INDEXLIB_MERGE_WORK_ITEM_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer_creator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/index/normal/framework/index_merger.h"
#include "indexlib/index/normal/source/source_group_merger.h"
#include "indexlib/index/normal/summary/summary_merger.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/merger/merge_work_item_typed.h"
#include "indexlib/merger/segment_directory.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResource);
DECLARE_REFERENCE_CLASS(index, TruncateIndexWriter);
DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);
DECLARE_REFERENCE_CLASS(file_system, ArchiveFolder);

namespace indexlib { namespace index { namespace legacy {
class AdaptiveBitmapIndexWriter;
}}} // namespace indexlib::index::legacy

namespace indexlib { namespace merger {

class MergeWorkItemCreator
{
public:
    MergeWorkItemCreator(const config::IndexPartitionSchemaPtr& schema, const config::MergeConfig& mergeConfig,
                         const IndexMergeMeta* mergeMeta, const merger::SegmentDirectoryPtr& segmentDirectory,
                         const merger::SegmentDirectoryPtr& subSegmentDirectory, bool isSortedMerge, bool optimize,
                         IndexPartitionMergerMetrics* metrics, const util::CounterMapPtr& counterMap,
                         const config::IndexPartitionOptions& options,
                         const merger::MergeFileSystemPtr& mergeFileSystem,
                         const plugin::PluginManagerPtr& pluginManager);
    virtual ~MergeWorkItemCreator();

public:
    // virtual for test
    virtual MergeWorkItem* CreateMergeWorkItem(const MergeTaskItem& item);

private:
    virtual index::DeletionMapMergerPtr CreateDeletionMapMerger() const;

    virtual index::SummaryMergerPtr CreateSummaryMerger(const MergeTaskItem& item,
                                                        const config::IndexPartitionSchemaPtr& schema) const;

    virtual index::SourceGroupMergerPtr CreateSourceGroupMerger(const MergeTaskItem& item,
                                                                const config::IndexPartitionSchemaPtr& schema) const;

    virtual index::AttributeMergerPtr CreateAttributeMerger(const MergeTaskItem& item,
                                                            const config::IndexPartitionSchemaPtr& schema) const;

    virtual index::IndexMergerPtr
    CreateIndexMerger(const MergeTaskItem& item, const config::IndexPartitionSchemaPtr& schema,
                      index::legacy::TruncateIndexWriterCreator* truncateCreator,
                      index::legacy::AdaptiveBitmapIndexWriterCreator* adaptiveCreator) const;

private:
    MergeWorkItem* CreateKeyValueMergeWorkItem(const MergeTaskItem& item);
    MergeWorkItem* DoCreateMergeWorkItem(const MergePlan& plan, const MergeTaskItem& item,
                                         const index_base::SegmentMergeInfos& inPlanSegMergeInfos,
                                         const index::ReclaimMapPtr& mainReclaimMap,
                                         const index::ReclaimMapPtr& subReclaimMap,
                                         const merger::SegmentDirectoryPtr& segmentDir,
                                         index_base::OutputSegmentMergeInfos outputSegmentMergeInfos);

    void PrepareDirectory(index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
                          const std::string& subPath) const;

private:
    void InitCreators(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    std::vector<index_base::MergeTaskResourcePtr> ExtractMergeTaskResource(const MergeTaskItem& item) const;
    void PrepareOutputSegMergeInfos(const MergeTaskItem& item, const MergePlan& plan,
                                    index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    void PrepareFolders();

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::MergeConfig mMergeConfig;
    const IndexMergeMeta* mMergeMeta;
    merger::SegmentDirectoryPtr mSegmentDirectory;
    merger::SegmentDirectoryPtr mSubSegDir;
    bool mIsSortedMerge;
    bool mIsOptimize;
    std::string mDumpPath;
    std::vector<index::legacy::TruncateAttributeReaderCreator*> mTruncateAttrReaderCreators;
    std::vector<index::legacy::TruncateIndexWriterCreator*> mTruncateIndexWriterCreators;
    std::vector<index::legacy::AdaptiveBitmapIndexWriterCreator*> mAdaptiveBitmapWriterCreators;
    std::vector<index::legacy::AdaptiveBitmapIndexWriterCreator*> mSubAdaptiveBitmapWriterCreators;
    file_system::ArchiveFolderPtr mTruncateMetaFolder;
    file_system::ArchiveFolderPtr mAdaptiveBitmapFolder;

    IndexPartitionMergerMetrics* mMetrics;
    util::CounterMapPtr mCounterMap;
    config::IndexPartitionOptions mOptions;
    int64_t mSampledKKVMergeMemUse;

    merger::MergeFileSystemPtr mMergeFileSystem;

    plugin::PluginManagerPtr mPluginManager;
    index_base::MergeTaskResourceManagerPtr mMergeTaskResourceMgr;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeWorkItemCreator);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_WORK_ITEM_CREATOR_H
