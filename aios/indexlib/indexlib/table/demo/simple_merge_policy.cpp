#include "indexlib/table/demo/simple_merge_policy.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, SimpleMergePolicy);

SimpleMergePolicy::SimpleMergePolicy(const util::KeyValueMap& parameters)
    : MergePolicy()
{
}

SimpleMergePolicy::~SimpleMergePolicy() 
{
}

vector<TableMergePlanPtr> SimpleMergePolicy::CreateMergePlansForIncrementalMerge(
        const string& mergeStrategyStr,
        const config::MergeStrategyParameter& mergeStrategyParameter,
        const vector<SegmentMetaPtr>& segmentMetas,
        const PartitionRange& targetRange) const
{
    vector<TableMergePlanPtr> emptyPlan;
    return emptyPlan;
}


vector<MergeTaskDescription> SimpleMergePolicy::CreateMergeTaskDescriptions(
    const TableMergePlanPtr& mergePlan,
    const TableMergePlanResourcePtr& planResource,
    const vector<SegmentMetaPtr>& segmentMetas,
    MergeSegmentDescription& segmentDescription) const
{
    vector<MergeTaskDescription> tasks;
    MergeTaskDescription taskDescription;
    taskDescription.name = "__default__";
    taskDescription.cost = 100;

    int64_t totalDocCount = 0;
    for (const auto& segMeta : segmentMetas)
    {
        totalDocCount += segMeta->segmentInfo.docCount;
    }
    segmentDescription.docCount = totalDocCount;
    segmentDescription.useSpecifiedDeployFileList = false;
    tasks.push_back(taskDescription);
    return tasks;
}


bool SimpleMergePolicy::ReduceMergeTasks(
    const TableMergePlanPtr& mergePlan,
    const vector<MergeTaskDescription>& taskDescriptions,
    const vector<DirectoryPtr>& inputDirectorys,
    const DirectoryPtr& outputDirectory, bool isFailOver) const
{
    // TODO: FileSystem support rename
    string segmentDataPath = outputDirectory->GetPath();
    for (const auto& inputDirectory : inputDirectorys)
    {
        fslib::FileList fileList;
        inputDirectory->ListFile("", fileList);
        try
        {
            for (const auto& filename : fileList)
            {
                inputDirectory->Rename(filename, outputDirectory);
            }        
        }
        catch(const misc::ExceptionBase& e)
        {
            return false;
        }        
    }
    return true;
}

IE_NAMESPACE_END(table);

