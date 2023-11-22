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

#include <stddef.h>
#include <vector>

#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/table/table_merge_plan_meta.h"
#include "indexlib/table/table_merge_plan_resource.h"

namespace indexlib { namespace table {

class TableMerger
{
public:
    TableMerger();
    virtual ~TableMerger();

public:
    virtual bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const TableMergePlanResourcePtr& mergePlanResources,
                      const TableMergePlanMetaPtr& mergePlanMeta) = 0;

    virtual size_t EstimateMemoryUse(const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
                                     const table::MergeTaskDescription& taskDescription) const = 0;

    virtual bool Merge(const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
                       const table::MergeTaskDescription& taskDescription,
                       const file_system::DirectoryPtr& outputDirectory) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMerger);
}} // namespace indexlib::table
