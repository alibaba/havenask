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
#include "indexlib/merger/merge_task_executor.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/merger/merge_work_item.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/merger/table_merge_work_item.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_merger.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;

using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::merger;
using namespace indexlib::table;
using namespace indexlib::file_system;

using namespace indexlib::util;
namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeTaskExecutor);

MergeTaskExecutor::MergeTaskExecutor(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                     const CounterMapPtr& counterMap, IndexPartitionMergerMetrics* metrics,
                                     bool isFullMerge, const merger::MergeFileSystemPtr& mergeFileSystem)
    : mSchema(schema)
    , mOptions(options)
    , mCounterMap(counterMap)
    , mMetrics(metrics)
    , mIsFullMerge(isFullMerge)
    , mMergeFileSystem(mergeFileSystem)
{
}

MergeTaskExecutor::~MergeTaskExecutor() {}

bool MergeTaskExecutor::Init(const TableMergeMetaPtr& tableMergeMeta, const TableFactoryWrapperPtr& tableFactory,
                             const vector<SegmentMetaPtr>& segmentMetas)
{
    if (!tableMergeMeta) {
        IE_LOG(ERROR, "TableMergeMeta is nullptr");
        return false;
    }
    if (!tableFactory) {
        IE_LOG(ERROR, "TableFactory is nullptr");
        return false;
    }
    mMergeMeta = tableMergeMeta;
    mTableFactory = tableFactory;

    for (const auto& segMeta : segmentMetas) {
        mSegMetaMap.insert(make_pair(segMeta->segmentDataBase.GetSegmentId(), segMeta));
    }
    return true;
}

MergeWorkItem* MergeTaskExecutor::CreateMergeWorkItem(const TaskExecuteMeta& taskExecMeta, bool& errorFlag) const
{
    errorFlag = false;
    TableMergerPtr tableMerger = mTableFactory->CreateTableMerger();
    string targetSegName =
        mMergeMeta->GetTargetVersion().GetSegmentDirName(mMergeMeta->GetTargetSegmentIds(taskExecMeta.planIdx)[0]);
    const auto& mergeConfig = mOptions.GetMergeConfig();
    // checkpoint logic
    string checkpointName = MergeTaskExecutor::GetCheckpointName(taskExecMeta);
    if (mergeConfig.enableMergeItemCheckPoint && mMergeFileSystem->HasCheckpoint(checkpointName)) {
        IE_LOG(INFO, "merge task item [%s] has already been done!", checkpointName.c_str());
        return NULL;
    }
    const std::string taskRelativeDir =
        util::PathUtil::JoinPath(targetSegName, MergeTaskExecutor::GetTaskDumpDir(taskExecMeta));
    auto rootDir = mMergeFileSystem->GetDirectory(/* root dir */ "");
    if (rootDir->IsExist(taskRelativeDir)) {
        // try remove from entryTable first
        rootDir->RemoveDirectory(taskRelativeDir);
    }
    mMergeFileSystem->MakeDirectory(taskRelativeDir);
    file_system::DirectoryPtr taskDumpDir = mMergeFileSystem->GetDirectory(taskRelativeDir);
    if (taskDumpDir == nullptr) {
        IE_LOG(INFO, "get directory failed, dir[%s], root[%s]", taskRelativeDir.c_str(),
               mMergeFileSystem->GetRootPath().c_str());
        return nullptr;
    }
    const auto& inPlanSegAttrs = mMergeMeta->mergePlans[taskExecMeta.planIdx]->GetInPlanSegments();

    vector<SegmentMetaPtr> inPlanSegMetas;
    inPlanSegMetas.reserve(inPlanSegAttrs.size());
    for (const auto segAttr : inPlanSegAttrs) {
        const auto& it = mSegMetaMap.find(segAttr.first);
        if (it == mSegMetaMap.end()) {
            IE_LOG(ERROR, "segmentid[%d] is not found in partition", segAttr.first);
            errorFlag = true;
            return NULL;
        }
        inPlanSegMetas.push_back(it->second);
    }

    std::unique_ptr<TableMergeWorkItem> workItem =
        std::make_unique<TableMergeWorkItem>(tableMerger, mSchema, mOptions, mMergeMeta, taskExecMeta, inPlanSegMetas,
                                             mIsFullMerge, taskDumpDir, mMergeFileSystem);

    if (!workItem->Init(mMetrics, mCounterMap)) {
        IE_LOG(ERROR, "init MergeWorkItem [planIdx:%u, taskIdx:%u] failed", taskExecMeta.planIdx, taskExecMeta.taskIdx);
        errorFlag = true;
        return NULL;
    }
    if (mergeConfig.enableMergeItemCheckPoint) {
        workItem->SetCheckPointFileName(checkpointName);
    }
    return workItem.release();
}

string MergeTaskExecutor::GetCheckpointName(const TaskExecuteMeta& taskExecMeta)
{
    return taskExecMeta.GetIdentifier();
}

string MergeTaskExecutor::GetTaskDumpDir(const TaskExecuteMeta& taskExecMeta)
{
    return MergeTaskExecutor::GetCheckpointName(taskExecMeta);
}
}} // namespace indexlib::merger
