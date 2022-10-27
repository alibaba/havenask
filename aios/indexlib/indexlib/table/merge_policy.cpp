#include "indexlib/table/merge_policy.h"

using namespace std;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, MergePolicy);

std::string MergePolicy::DEFAULT_MERGE_TASK_NAME = "__task_default__";

MergePolicy::MergePolicy() 
{
}

MergePolicy::~MergePolicy() 
{
}

bool MergePolicy::Init(const config::IndexPartitionSchemaPtr& schema,
                       const config::IndexPartitionOptions& options)
{
    mSchema = schema;
    mOptions = options;
    return DoInit();
}

bool MergePolicy::DoInit()
{
    return true;
}

TableMergePlanResourcePtr MergePolicy::CreateMergePlanResource() const
{
    TableMergePlanResourcePtr emptyResource;
    return emptyResource;
}

vector<TableMergePlanPtr> MergePolicy::CreateMergePlansForFullMerge(
    const string& mergeStrategyStr,
    const MergeStrategyParameter& mergeStrategyParameter,
    const vector<SegmentMetaPtr>& allSegmentMetas,
    const PartitionRange& targetRange) const
{
    TableMergePlanPtr newPlan(new TableMergePlan());
    for (const auto& segMeta : allSegmentMetas)
    {
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

IE_NAMESPACE_END(table);

