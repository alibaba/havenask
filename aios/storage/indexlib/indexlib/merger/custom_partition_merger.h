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
#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/dump_strategy.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/table/merge_policy.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);

namespace indexlib { namespace merger {

class CustomPartitionMerger : public IndexPartitionMerger
{
public:
    CustomPartitionMerger(const SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema,
                          const config::IndexPartitionOptions& options, const DumpStrategyPtr& dumpStrategy,
                          util::MetricProviderPtr metricProvider, const table::TableFactoryWrapperPtr& tableFactory,
                          const index_base::CommonBranchHinterOption& branchOption,
                          const PartitionRange& range = PartitionRange());
    ~CustomPartitionMerger();

public:
    MergeMetaPtr CreateMergeMeta(bool optimize, uint32_t instanceCount, int64_t currentTs) override;
    MergeMetaPtr LoadMergeMeta(const std::string& mergeMetaPath, bool onlyLoadBasicInfo) override;
    void DoMerge(bool optimize, const MergeMetaPtr& mergeMeta, uint32_t instanceId) override;
    void EndMerge(const MergeMetaPtr& mergeMeta, versionid_t alignVersionId = INVALID_VERSIONID) override;

private:
    bool FilterEmptyTableMergePlan(std::vector<table::TableMergePlanPtr>& tableMergePlans);

    bool CreateSegmentMetas(const merger::SegmentDirectoryPtr& segmentDirectory,
                            std::vector<table::SegmentMetaPtr>& segMetas) const;

    table::TableMergePlanMetaPtr CreateTableMergePlanMeta(const std::vector<table::SegmentMetaPtr>& segmentMetas,
                                                          segmentid_t targetSegmentId) const;

    std::vector<index_base::SegmentInfo>
    GetLastSegmentInfosForMultiPartitionMerge(const MultiPartSegmentDirectoryPtr& multiPartSegDir,
                                              const std::vector<table::SegmentMetaPtr>& segmentMetas) const;

    index_base::Version CreateNewVersion(const std::vector<table::TableMergePlanPtr>& mergePlans,
                                         const std::vector<table::TableMergePlanMetaPtr>& mergePlanMetas);

    merger::TableMergeMetaPtr CreateTableMergeMeta(const table::MergePolicyPtr& mergePolicy, bool isFullMerge,
                                                   uint32_t instanceCount, int64_t currentTs);

    void CreateInPlanSegmentMetas(const std::vector<table::SegmentMetaPtr> allSegMetas,
                                  const table::TableMergePlanPtr& mergePlan,
                                  std::vector<table::SegmentMetaPtr>& inPlanSegmentMetas);

    bool ValidateMergeTaskDescriptions(const table::MergeTaskDescriptions& taskDescriptions);

    bool MergeInstanceDir(const file_system::DirectoryPtr& rootDir, const merger::TableMergeMetaPtr& mergeMeta);
    void RemoveInstanceDir(const file_system::DirectoryPtr& rootDir) const;

    bool CollectMergeTaskDirs(const file_system::DirectoryPtr& baseDir, const file_system::FileList& instanceDirList,
                              const merger::TableMergeMetaPtr& mergeMeta,
                              std::vector<std::vector<file_system::DirectoryPtr>>& mergeTaskDirs);

    index_base::SegmentInfo CreateSegmentInfo(const table::TableMergePlanMetaPtr& mergePlanMeta) const;

private:
    table::TableFactoryWrapperPtr mTableFactory;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomPartitionMerger);
}} // namespace indexlib::merger
