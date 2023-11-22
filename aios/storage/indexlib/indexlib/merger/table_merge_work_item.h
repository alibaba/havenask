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
#include <vector>

#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/merger/merge_work_item.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/table/task_execute_meta.h"

DECLARE_REFERENCE_CLASS(table, TableMerger);
DECLARE_REFERENCE_CLASS(table, TableMergePlan);
DECLARE_REFERENCE_CLASS(table, TableMergePlanMeta);
DECLARE_REFERENCE_CLASS(table, SegmentMeta);
DECLARE_REFERENCE_CLASS(merger, TableMergeMeta);

namespace indexlib { namespace merger {

class TableMergeWorkItem : public MergeWorkItem
{
public:
    TableMergeWorkItem(const table::TableMergerPtr& tableMerger, const config::IndexPartitionSchemaPtr& schema,
                       const config::IndexPartitionOptions& options, const merger::TableMergeMetaPtr& mergeMeta,
                       const table::TaskExecuteMeta& execMeta, const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
                       bool isFullMerge, const file_system::DirectoryPtr& taskDumpDir,
                       const merger::MergeFileSystemPtr& mergeFileSystem);
    ~TableMergeWorkItem();

public:
    bool Init(merger::IndexPartitionMergerMetrics* metrics, const util::CounterMapPtr& counterMap);
    int64_t GetRequiredResource() const override;
    int64_t EstimateMemoryUse() const override;
    void Destroy() override;

private:
    void DoProcess() override;

private:
    table::TableMergerPtr mTableMerger;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    merger::TableMergeMetaPtr mMergeMeta;
    table::TaskExecuteMeta mTaskExecMeta;
    std::vector<table::SegmentMetaPtr> mInPlanSegMetas;
    file_system::DirectoryPtr mTaskDumpDir;
    int64_t mEstimateMemUse;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergeWorkItem);
}} // namespace indexlib::merger
