#include "indexlib/table/demo/demo_merge_policy.h"
#include "indexlib/table/demo/demo_table_writer.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, DemoMergePolicy);

DemoMergePolicy::DemoMergePolicy(const util::KeyValueMap& parameters) 
{
}

DemoMergePolicy::~DemoMergePolicy() 
{
}

vector<TableMergePlanPtr> DemoMergePolicy::CreateMergePlansForIncrementalMerge(
        const string& mergeStrategyStr,
        const config::MergeStrategyParameter& mergeStrategyParameter,
        const vector<SegmentMetaPtr>& allSegmentMetas,
        const PartitionRange& targetRange) const
{
    vector<TableMergePlanPtr> emptyPlan;
    return emptyPlan;
}

vector<MergeTaskDescription> DemoMergePolicy::CreateMergeTaskDescriptions(
    const TableMergePlanPtr& mergePlan,
    const TableMergePlanResourcePtr& planResource,
    const vector<SegmentMetaPtr>& inPlanSegmentMetas,
    MergeSegmentDescription& segmentDescription) const
{
    vector<MergeTaskDescription> tasks;
    MergeTaskDescription taskDescription;
    taskDescription.name = "__default__";
    taskDescription.cost = 100;

    int64_t totalDocCount = 0;
    for (const auto& segMeta : inPlanSegmentMetas)
    {
        totalDocCount += segMeta->segmentInfo.docCount;
    }
    segmentDescription.docCount = totalDocCount;
    segmentDescription.useSpecifiedDeployFileList = true;
    segmentDescription.deployFileList.push_back(DemoTableWriter::DATA_FILE_NAME);
    tasks.push_back(taskDescription);
    return tasks;
}

bool DemoMergePolicy::ReduceMergeTasks(
    const TableMergePlanPtr& mergePlan,
    const vector<MergeTaskDescription>& taskDescriptions,
    const vector<DirectoryPtr>& inputDirectorys,
    const DirectoryPtr& outputDirectory, bool isFailOver) const
{
    if (taskDescriptions.size() != 1)
    {
        return false;
    }
    
    fslib::FileList fileList;
    inputDirectorys[0]->ListFile("", fileList);
    try
    {
        for (const auto& fileName : fileList)
        {
            inputDirectorys[0]->Rename(fileName, outputDirectory);
        }        
    }
    catch(const misc::ExceptionBase& e)
    {
        return false;
    }
    return true;
}

IE_NAMESPACE_END(table);

