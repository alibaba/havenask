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
#include "indexlib/merger/table_merge_work_item.h"

#include <ext/alloc_traits.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/table/table_merge_plan_meta.h"
#include "indexlib/table/table_merger.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "kmonitor/client/core/MetricsTags.h"

using namespace std;

using namespace indexlib::table;
using namespace indexlib::common;
using namespace indexlib::config;

using namespace indexlib::util;
namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, TableMergeWorkItem);

TableMergeWorkItem::TableMergeWorkItem(const TableMergerPtr& tableMerger, const IndexPartitionSchemaPtr& schema,
                                       const IndexPartitionOptions& options, const TableMergeMetaPtr& mergeMeta,
                                       const TaskExecuteMeta& execMeta, const vector<SegmentMetaPtr>& inPlanSegMetas,
                                       bool isFullMerge, const file_system::DirectoryPtr& taskDumpDir,
                                       const merger::MergeFileSystemPtr& mergeFileSystem)
    : MergeWorkItem(mergeFileSystem)
    , mTableMerger(tableMerger)
    , mSchema(schema)
    , mOptions(options)
    , mMergeMeta(mergeMeta)
    , mTaskExecMeta(execMeta)
    , mInPlanSegMetas(inPlanSegMetas)
    , mTaskDumpDir(taskDumpDir)
    , mEstimateMemUse(0)
{
}

TableMergeWorkItem::~TableMergeWorkItem() {}

bool TableMergeWorkItem::Init(IndexPartitionMergerMetrics* metrics, const CounterMapPtr& counterMap)
{
    if (!mTableMerger) {
        IE_LOG(ERROR, "fail to create TableMerger");
        return false;
    }

    if (!mTableMerger->Init(mSchema, mOptions, mMergeMeta->mergePlanResources[mTaskExecMeta.planIdx],
                            mMergeMeta->mergePlanMetas[mTaskExecMeta.planIdx])) {
        IE_LOG(ERROR, "init TableMerger failed");
        return false;
    }
    const auto& taskDescription = mMergeMeta->mergeTaskDescriptions[mTaskExecMeta.planIdx][mTaskExecMeta.taskIdx];

    if (counterMap) {
        string counterPath = "offline.mergeTime.TableMergeWorkItem.MergePlan" +
                             autil::StringUtil::toString(mTaskExecMeta.planIdx) + "]" + "[" + taskDescription.name +
                             "]";
        const auto& counter = counterMap->GetStateCounter(counterPath);
        if (!counter) {
            IE_LOG(ERROR, "init counter[%s] failed", counterPath.c_str());
        } else {
            SetCounter(counter);
        }
    }
    mEstimateMemUse = static_cast<int64_t>(mTableMerger->EstimateMemoryUse(mInPlanSegMetas, taskDescription));
    SetIdentifier(taskDescription.name);

    kmonitor::MetricsTags tags;
    tags.AddTag("schema_name", mSchema->GetSchemaName());
    tags.AddTag("item_id", taskDescription.name);
    auto inPlanSegments = mMergeMeta->mergePlans[mTaskExecMeta.planIdx]->GetInPlanSegments();
    string fromSegStr;
    for (auto inPlanSegIter = inPlanSegments.begin(); inPlanSegIter != inPlanSegments.end(); inPlanSegIter++) {
        if (inPlanSegIter != inPlanSegments.begin()) {
            fromSegStr += ",";
        }
        fromSegStr += autil::StringUtil::toString(inPlanSegIter->first);
    }
    tags.AddTag("merge_from", fromSegStr);
    segmentid_t targetSegment = mMergeMeta->mergePlanMetas[mTaskExecMeta.planIdx]->targetSegmentId;
    tags.AddTag("merge_to", autil::StringUtil::toString(targetSegment));
    SetMetrics(metrics, static_cast<double>(taskDescription.cost), tags);
    return true;
}
void TableMergeWorkItem::Destroy() { mTableMerger.reset(); }

void TableMergeWorkItem::DoProcess()
{
    try {
        int64_t timeBeforeMerge = autil::TimeUtility::currentTimeInMicroSeconds();
        IE_LOG(INFO, "Begin merging %s", GetIdentifier().c_str());
        const auto& taskDescription = mMergeMeta->mergeTaskDescriptions[mTaskExecMeta.planIdx][mTaskExecMeta.taskIdx];
        if (!mTableMerger->Merge(mInPlanSegMetas, taskDescription, mTaskDumpDir)) {
            INDEXLIB_FATAL_ERROR(Runtime, "Merge failed for task[%s] in mergePlan[%u]", taskDescription.name.c_str(),
                                 mTaskExecMeta.planIdx);
        }
        int64_t timeAfterMerge = autil::TimeUtility::currentTimeInMicroSeconds();
        float useTime = (timeAfterMerge - timeBeforeMerge) / 1000000.0;
        IE_LOG(INFO, "Finish merging %s, use [%.3f] seconds, cost [%.2f]", GetIdentifier().c_str(), useTime, GetCost());
        if (mCounter) {
            mCounter->Set(useTime);
        }
        ReportMetrics();
    } catch (util::ExceptionBase& e) {
        IE_LOG(ERROR, "Exception throwed when merging %s : %s", GetIdentifier().c_str(), e.ToString().c_str());
        throw;
    }
}

int64_t TableMergeWorkItem::GetRequiredResource() const { return mEstimateMemUse; }

int64_t TableMergeWorkItem::EstimateMemoryUse() const { return mEstimateMemUse; }
}} // namespace indexlib::merger
