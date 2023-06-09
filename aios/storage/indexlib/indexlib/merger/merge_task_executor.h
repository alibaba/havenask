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
#ifndef __INDEXLIB_MERGE_TASK_EXECUTOR_H
#define __INDEXLIB_MERGE_TASK_EXECUTOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/task_execute_meta.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(merger, IndexPartitionMergerMetrics);
DECLARE_REFERENCE_CLASS(merger, MergeWorkItem);
DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);
DECLARE_REFERENCE_CLASS(merger, TableMergeMeta);
DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(table, SegmentMeta);
DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace indexlib { namespace merger {

class MergeTaskExecutor
{
public:
    MergeTaskExecutor(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const util::CounterMapPtr& counterMap, merger::IndexPartitionMergerMetrics* metrics,
                      bool isFullMerge, const merger::MergeFileSystemPtr& mergeFileSystem);
    ~MergeTaskExecutor();

public:
    bool Init(const merger::TableMergeMetaPtr& tableMergeMeta, const table::TableFactoryWrapperPtr& tableFactory,
              const std::vector<table::SegmentMetaPtr>& segmentMetas);

    merger::MergeWorkItem* CreateMergeWorkItem(const table::TaskExecuteMeta& taskExecMeta, bool& errorFlag) const;

private:
    static std::string GetCheckpointName(const table::TaskExecuteMeta& taskExecMeta);
    static std::string GetTaskDumpDir(const table::TaskExecuteMeta& taskExecMeta);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    util::CounterMapPtr mCounterMap;
    merger::IndexPartitionMergerMetrics* mMetrics;
    bool mIsFullMerge;

    merger::TableMergeMetaPtr mMergeMeta;
    table::TableFactoryWrapperPtr mTableFactory;
    std::map<segmentid_t, table::SegmentMetaPtr> mSegMetaMap;
    merger::MergeFileSystemPtr mMergeFileSystem;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTaskExecutor);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_TASK_EXECUTOR_H
