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

#include <string>
#include <vector>

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/merge_task_dispatcher.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/table/table_common.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/table/table_merge_plan_resource.h"

namespace indexlib { namespace table {

class MergePolicy
{
public:
    MergePolicy();
    virtual ~MergePolicy();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options);

    virtual bool DoInit();

    virtual std::vector<TableMergePlanPtr> CreateMergePlansForFullMerge(
        const std::string& mergeStrategyStr, const config::MergeStrategyParameter& mergeStrategyParameter,
        const std::vector<SegmentMetaPtr>& allSegmentMetas, const PartitionRange& targetRange) const;

    virtual std::vector<TableMergePlanPtr> CreateMergePlansForIncrementalMerge(
        const std::string& mergeStrategyStr, const config::MergeStrategyParameter& mergeStrategyParameter,
        const std::vector<SegmentMetaPtr>& allSegmentMetas, const PartitionRange& targetRange) const = 0;

    virtual TableMergePlanResourcePtr CreateMergePlanResource() const;

    virtual std::vector<MergeTaskDescription>
    CreateMergeTaskDescriptions(const index_base::Version& baseVersion, const TableMergePlanPtr& mergePlan,
                                const TableMergePlanResourcePtr& planResource,
                                const std::vector<SegmentMetaPtr>& inPlanSegmentMetas,
                                MergeSegmentDescription& segmentDescription) const = 0;

    // virtual bool ReduceMergeTasks(const TableMergePlanPtr& mergePlan,
    //                               const std::vector<MergeTaskDescription>& taskDescriptions,
    //                               const std::vector<std::string>& mergeTaskDumpPaths,
    //                               const std::string& segmentDataPath, bool isFailOver) const = 0;
    virtual bool ReduceMergeTasks(const TableMergePlanPtr& mergePlan,
                                  const std::vector<MergeTaskDescription>& taskDescriptions,
                                  const std::vector<file_system::DirectoryPtr>& inputDirectorys,
                                  const file_system::DirectoryPtr& outputDirectory, bool isFailOver) const = 0;

    virtual MergeTaskDispatcherPtr CreateMergeTaskDispatcher() const;

protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

public:
    static std::string DEFAULT_MERGE_TASK_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergePolicy);
}} // namespace indexlib::table
