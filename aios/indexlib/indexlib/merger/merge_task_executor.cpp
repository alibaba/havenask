#include "indexlib/merger/merge_task_executor.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/merger/merge_work_item.h"
#include "indexlib/merger/table_merge_work_item.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/table/table_merger.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/path_util.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeTaskExecutor);

MergeTaskExecutor::MergeTaskExecutor(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const CounterMapPtr& counterMap,
        IndexPartitionMergerMetrics* metrics,
        bool isFullMerge,
        const merger::MergeFileSystemPtr& mergeFileSystem)
    : mSchema(schema)
    , mOptions(options)
    , mCounterMap(counterMap)
    , mMetrics(metrics)
    , mIsFullMerge(isFullMerge)
    , mMergeFileSystem(mergeFileSystem)
{}

MergeTaskExecutor::~MergeTaskExecutor() 
{
}

bool MergeTaskExecutor::Init(
    const TableMergeMetaPtr& tableMergeMeta,
    const TableFactoryWrapperPtr& tableFactory,
    const vector<SegmentMetaPtr>& segmentMetas)
{
    if (!tableMergeMeta)
    {
        IE_LOG(ERROR, "TableMergeMeta is nullptr");
        return false;
    }
    if (!tableFactory)
    {
        IE_LOG(ERROR, "TableFactory is nullptr");
        return false;
    }
    mMergeMeta = tableMergeMeta;
    mTableFactory = tableFactory;

    for (const auto& segMeta : segmentMetas)
    {
        mSegMetaMap.insert(make_pair(segMeta->segmentDataBase.GetSegmentId(),
                                     segMeta));
    }
    return true;
}

MergeWorkItem* MergeTaskExecutor::CreateMergeWorkItem(
    const TaskExecuteMeta& taskExecMeta, bool& errorFlag) const
{
    errorFlag = false;
    TableMergerPtr tableMerger = mTableFactory->CreateTableMerger();
    string targetSegName = mMergeMeta->GetTargetVersion().GetSegmentDirName(
        mMergeMeta->GetTargetSegmentIds(taskExecMeta.planIdx)[0]);
    const auto& mergeConfig = mOptions.GetMergeConfig();
    // checkpoint logic
    string checkpointName = MergeTaskExecutor::GetCheckpointName(taskExecMeta);
    if (mergeConfig.enableMergeItemCheckPoint &&
        mMergeFileSystem->HasCheckpoint(checkpointName))
    {
        IE_LOG(INFO, "merge task item [%s] has already been done!", checkpointName.c_str());
        return NULL;
    }
    string taskDumpPath = util::PathUtil::JoinPath(mMergeFileSystem->GetRootPath(),
            targetSegName, MergeTaskExecutor::GetTaskDumpDir(taskExecMeta));

    FileSystemWrapper::DeleteIfExist(taskDumpPath);
    mMergeFileSystem->MakeDirectory(taskDumpPath, true);    

    const auto& inPlanSegAttrs =
        mMergeMeta->mergePlans[taskExecMeta.planIdx]->GetInPlanSegments();

    vector<SegmentMetaPtr> inPlanSegMetas;
    inPlanSegMetas.reserve(inPlanSegAttrs.size());
    for (const auto segAttr : inPlanSegAttrs)
    {
        const auto& it = mSegMetaMap.find(segAttr.first);
        if (it == mSegMetaMap.end())
        {
            IE_LOG(ERROR, "segmentid[%d] is not found in partition", segAttr.first);
            errorFlag = true;
            return NULL;
        }
        inPlanSegMetas.push_back(it->second);
    }

    TableMergeWorkItem* workItem = new TableMergeWorkItem(
        tableMerger, mSchema,
        mOptions, mMergeMeta,
        taskExecMeta, inPlanSegMetas,
        mIsFullMerge, taskDumpPath, mMergeFileSystem);

    if (!workItem->Init(mMetrics, mCounterMap))
    {
        IE_LOG(ERROR, "init MergeWorkItem [planIdx:%u, taskIdx:%u] failed",
               taskExecMeta.planIdx, taskExecMeta.taskIdx);
        errorFlag = true;
        return NULL;
    }
    if (mergeConfig.enableMergeItemCheckPoint)
    {
        workItem->SetCheckPointFileName(checkpointName);
    }
    return workItem;
}

string MergeTaskExecutor::GetCheckpointName(
    const TaskExecuteMeta& taskExecMeta)
{
    return taskExecMeta.GetIdentifier();
}

string MergeTaskExecutor::GetTaskDumpDir(
    const TaskExecuteMeta& taskExecMeta)
{
    return MergeTaskExecutor::GetCheckpointName(taskExecMeta);
}


IE_NAMESPACE_END(merger);

