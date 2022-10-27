#include "indexlib/merger/table_merge_work_item.h"
#include "indexlib/table/table_merger.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/table/table_merge_plan_meta.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/util/counter/counter_map.h"

using namespace std;

IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, TableMergeWorkItem);

TableMergeWorkItem::TableMergeWorkItem(
    const TableMergerPtr& tableMerger,
    const IndexPartitionSchemaPtr& schema,
    const IndexPartitionOptions& options,
    const TableMergeMetaPtr& mergeMeta,
    const TaskExecuteMeta& execMeta,
    const vector<SegmentMetaPtr>& inPlanSegMetas,
    bool isFullMerge,
    const string& taskDumpPath,
    const merger::MergeFileSystemPtr& mergeFileSystem)
    : MergeWorkItem(mergeFileSystem)
    , mTableMerger(tableMerger)
    , mSchema(schema)
    , mOptions(options)
    , mMergeMeta(mergeMeta)
    , mTaskExecMeta(execMeta)
    , mInPlanSegMetas(inPlanSegMetas)
    , mIsFullMerge(isFullMerge)
    , mTaskDumpPath(taskDumpPath)
    , mEstimateMemUse(0)
{
}

TableMergeWorkItem::~TableMergeWorkItem() 
{
}

bool TableMergeWorkItem::Init(IndexPartitionMergerMetrics* metrics,
                              const CounterMapPtr& counterMap)
{
    if (!mTableMerger)
    {
        IE_LOG(ERROR, "fail to create TableMerger");
        return false;
    }
    
    if (!mTableMerger->Init(
            mSchema, mOptions,
            mMergeMeta->mergePlanResources[mTaskExecMeta.planIdx],
            mMergeMeta->mergePlanMetas[mTaskExecMeta.planIdx]))
    {
        IE_LOG(ERROR, "init TableMerger failed");
        return false;
    }
    const auto& taskDescription =
        mMergeMeta->mergeTaskDescriptions[mTaskExecMeta.planIdx][mTaskExecMeta.taskIdx];

    if (counterMap)
    {
        string counterPath = "offline.mergeTime.TableMergeWorkItem.MergePlan"
            + autil::StringUtil::toString(mTaskExecMeta.planIdx) + "]"
            + "[" + taskDescription.name + "]";
        const auto& counter = counterMap->GetStateCounter(counterPath);
        if (!counter)
        {
            IE_LOG(ERROR, "init counter[%s] failed", counterPath.c_str());
        }
        else
        {
            SetCounter(counter);
        }        
    }
    mEstimateMemUse = static_cast<int64_t>(mTableMerger->EstimateMemoryUse(mInPlanSegMetas, taskDescription));
    SetIdentifier(taskDescription.name);
    SetMetrics(metrics, static_cast<double>(taskDescription.cost));
    return true;
}
void TableMergeWorkItem::Destroy()
{
    mTableMerger.reset();
}
    
void TableMergeWorkItem::DoProcess()
{
    try
    {
        int64_t timeBeforeMerge = autil::TimeUtility::currentTimeInMicroSeconds();
        IE_LOG(INFO, "Begin merging %s", GetIdentifier().c_str());
        const file_system::DirectoryPtr& outputDirectory = mMergeFileSystem->GetDirectory(mTaskDumpPath);
        const auto& taskDescription =
            mMergeMeta->mergeTaskDescriptions[mTaskExecMeta.planIdx][mTaskExecMeta.taskIdx];        
        if (!mTableMerger->Merge(mInPlanSegMetas, taskDescription, outputDirectory))
        {
            INDEXLIB_FATAL_ERROR(Runtime, "Merge failed for task[%s] in mergePlan[%u]",
                                 taskDescription.name.c_str(), mTaskExecMeta.planIdx);
        }
        int64_t timeAfterMerge = autil::TimeUtility::currentTimeInMicroSeconds();
        float useTime = (timeAfterMerge - timeBeforeMerge) / 1000000.0;
        IE_LOG(INFO, "Finish merging %s, use [%.3f] seconds, cost [%.2f]",
               GetIdentifier().c_str(), useTime, GetCost());
        if (mCounter)
        {
            mCounter->Set(useTime);
        }
        ReportMetrics();
    }
    catch (misc::ExceptionBase& e)
    {
        IE_LOG(ERROR, "Exception throwed when merging %s : %s", 
               GetIdentifier().c_str(), e.ToString().c_str());
        throw;
    }
}

int64_t TableMergeWorkItem::GetRequiredResource() const
{
    return mEstimateMemUse;
}

int64_t TableMergeWorkItem::EstimateMemoryUse() const
{
    return mEstimateMemUse;
}

IE_NAMESPACE_END(merger);

