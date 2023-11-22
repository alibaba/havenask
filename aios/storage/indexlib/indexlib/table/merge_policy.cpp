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
#include "indexlib/table/merge_policy.h"

#include <algorithm>
#include <iosfwd>
#include <memory>

#include "indexlib/config/load_config_list.h"
#include "indexlib/index_base/segment/segment_data_base.h"

using namespace std;

using namespace indexlib::config;

namespace indexlib { namespace table {
IE_LOG_SETUP(table, MergePolicy);

std::string MergePolicy::DEFAULT_MERGE_TASK_NAME = "__task_default__";

MergePolicy::MergePolicy() {}

MergePolicy::~MergePolicy() {}

bool MergePolicy::Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options)
{
    mSchema = schema;
    mOptions = options;
    return DoInit();
}

bool MergePolicy::DoInit() { return true; }

TableMergePlanResourcePtr MergePolicy::CreateMergePlanResource() const
{
    TableMergePlanResourcePtr emptyResource;
    return emptyResource;
}

vector<TableMergePlanPtr> MergePolicy::CreateMergePlansForFullMerge(
    const string& mergeStrategyStr, const MergeStrategyParameter& mergeStrategyParameter,
    const vector<SegmentMetaPtr>& allSegmentMetas, const PartitionRange& targetRange) const
{
    TableMergePlanPtr newPlan(new TableMergePlan());
    for (const auto& segMeta : allSegmentMetas) {
        newPlan->AddSegment(segMeta->segmentDataBase.GetSegmentId());
    }
    vector<TableMergePlanPtr> newPlans;
    newPlans.push_back(newPlan);
    return newPlans;
}

MergeTaskDispatcherPtr MergePolicy::CreateMergeTaskDispatcher() const
{
    return MergeTaskDispatcherPtr(new MergeTaskDispatcher());
}
}} // namespace indexlib::table
