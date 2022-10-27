#include "indexlib/merger/parallel_end_merge_executor.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index/normal/summary/summary_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_define.h"
#include <numeric>

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, ParallelEndMergeExecutor);

ParallelEndMergeExecutor::ParallelEndMergeExecutor(
        const config::IndexPartitionSchemaPtr &schema,
        const plugin::PluginManagerPtr& pluginManager,
        const file_system::DirectoryPtr& rootDirectory)
    : mSchema(schema)
    , mPluginManager(pluginManager)
    , mRootDirectory(rootDirectory)
{
}

ParallelEndMergeExecutor::~ParallelEndMergeExecutor() 
{
}

static bool CompareMergeTaskItemByTaskId(const MergeTaskItem& lft,
        const MergeTaskItem& rht)
{
    return lft.GetParallelMergeItem().GetTaskGroupId() <
        rht.GetParallelMergeItem().GetTaskGroupId();
}

void ParallelEndMergeExecutor::ParallelEndMerge(const IndexMergeMetaPtr& mergeMeta)
{
    vector<MergeTaskItems> taskGroups = ExtractParallelMergeTaskGroup(mergeMeta);
    for (size_t i = 0; i < taskGroups.size(); i++)
    {
        const MergeTaskItems& taskGroup = taskGroups[i];
        SingleGroupParallelEndMerge(mergeMeta, taskGroup);
    }
}


void ParallelEndMergeExecutor::SingleGroupParallelEndMerge(
        const IndexMergeMetaPtr& mergeMeta, const MergeTaskItems& taskGroup)
{
    MergeTaskItem item = taskGroup[0];
    item.SetParallelMergeItem(ParallelMergeItem());

    MergeTaskResourceManagerPtr mgr = mergeMeta->CreateMergeTaskResourceManager();
    vector<MergeTaskResourceVector> resourceVec = ExtractMergeTaskResource(mgr, taskGroup);
    const MergePlan& plan = mergeMeta->GetMergePlan(item.mMergePlanIdx);
    const Version& targetVersion = mergeMeta->GetTargetVersion();
    auto targetSegmentCount = plan.GetTargetSegmentCount();

    auto segIdx = item.mTargetSegmentIdx;
    if (segIdx != -1)
    {
        // multi output segment parallel
        return;
    }
    OutputSegmentMergeInfos outputSegMergeInfos;
    for (size_t i = 0; i < targetSegmentCount; ++i)
    {
        OutputSegmentMergeInfo info;
        info.targetSegmentId = plan.GetTargetSegmentId(i);
        info.targetSegmentIndex = i;

        string segDirName = targetVersion.GetSegmentDirName(info.targetSegmentId);

        info.path = FileSystemWrapper::JoinPath(mRootDirectory->GetPath(), segDirName);
        info.directory = mRootDirectory->GetDirectory(segDirName, true);
        outputSegMergeInfos.push_back(info);
    }

    const IndexPartitionSchemaPtr& schema
        = item.mIsSubItem ? mSchema->GetSubIndexPartitionSchema() : mSchema;
    auto joinDirectory = [&outputSegMergeInfos](const std::string& dirName) {
        for (auto& info : outputSegMergeInfos)
        {
            info.path = FileSystemWrapper::JoinPath(info.path, dirName);
            info.directory = info.directory->GetDirectory(dirName, true);
        }
    };
    if (item.mMergeType == SUMMARY_TASK_NAME)
    {
        joinDirectory(SUMMARY_DIR_NAME);
        SummaryMergerPtr summaryMerger = CreateSummaryMerger(item, schema);
        summaryMerger->EndParallelMerge(outputSegMergeInfos, taskGroup.size(), resourceVec);
    }
    else if (item.mMergeType == INDEX_TASK_NAME)
    {
        joinDirectory(INDEX_DIR_NAME);
        IndexMergerPtr indexMerger = CreateIndexMerger(item, schema);
        indexMerger->EndParallelMerge(outputSegMergeInfos, taskGroup.size(), resourceVec);
    }
    else if (item.mMergeType == ATTRIBUTE_TASK_NAME
             || item.mMergeType == PACK_ATTRIBUTE_TASK_NAME)
    {
        joinDirectory(ATTRIBUTE_DIR_NAME);
        AttributeMergerPtr attrMerger = CreateAttributeMerger(item, schema);
        attrMerger->EndParallelMerge(outputSegMergeInfos, taskGroup.size(), resourceVec);
    }
    else
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "merge task item [%s] not support parallel merge!",
                             item.ToString().c_str());
    }

}

vector<MergeTaskItems> ParallelEndMergeExecutor::ExtractParallelMergeTaskGroup(
        const IndexMergeMetaPtr& mergeMeta)
{
    assert(mergeMeta);
    const vector<MergeTaskItems>& mergeTaskItems = mergeMeta->GetMergeTaskItemsVec();
    MergeTaskItems items;
    for (size_t i = 0; i < mergeTaskItems.size(); i++)
    {
        for (auto& item : mergeTaskItems[i])
        {
            if (item.GetParallelMergeItem().GetTaskGroupId() ==
                ParallelMergeItem::INVALID_TASK_GROUP_ID)
            {
                continue;
            }
            if (item.GetParallelMergeItem().GetTotalParallelCount() > 1)
            {
                items.push_back(item);
            }
        }
    }
    sort(items.begin(), items.end(), CompareMergeTaskItemByTaskId);
    vector<MergeTaskItems> taskGroups;
    MergeTaskItems taskGroup;
    int32_t groupId = ParallelMergeItem::INVALID_TASK_GROUP_ID;
    for (size_t i = 0; i < items.size(); i++)
    {
        if (groupId == items[i].GetParallelMergeItem().GetTaskGroupId())
        {
            taskGroup.push_back(items[i]);
            continue;
        }
        if (!taskGroup.empty())
        {
            CheckOneTaskGroup(taskGroup);
            taskGroups.push_back(taskGroup);
        }
        taskGroup.clear();
        groupId = items[i].GetParallelMergeItem().GetTaskGroupId();
        taskGroup.push_back(items[i]);
    }
    if (!taskGroup.empty())
    {
        CheckOneTaskGroup(taskGroup);
        taskGroups.push_back(taskGroup);
    }
    return taskGroups;
}

void ParallelEndMergeExecutor::CheckOneTaskGroup(const MergeTaskItems& taskGroup)
{
    if (taskGroup.size() != (size_t)
        taskGroup[0].GetParallelMergeItem().GetTotalParallelCount())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "task group [%d] : task item count [%lu] not match with parallelCount [%d]",
                             taskGroup[0].GetParallelMergeItem().GetTaskGroupId(),
                             taskGroup.size(),
                             taskGroup[0].GetParallelMergeItem().GetTotalParallelCount());
    }

    assert(taskGroup.size() > 1);
    for (size_t i = 1; i < taskGroup.size(); i++)
    {
        if (taskGroup[i].mMergePlanIdx != taskGroup[0].mMergePlanIdx)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "task group [%d] : merge plan idx not match [%u:%u]",
                    taskGroup[0].GetParallelMergeItem().GetTaskGroupId(),
                    taskGroup[i].mMergePlanIdx, taskGroup[0].mMergePlanIdx);
        }
    }
}
    
SummaryMergerPtr ParallelEndMergeExecutor::CreateSummaryMerger(
        const MergeTaskItem &item, 
        const IndexPartitionSchemaPtr &schema) const
{
    // for compatible
    string summaryGroupName = item.mName.empty() ? DEFAULT_SUMMARYGROUPNAME : item.mName;
    assert(schema && schema->GetSummarySchema());
    const SummarySchemaPtr &summarySchema = schema->GetSummarySchema();
    return SummaryMergerPtr(new LocalDiskSummaryMerger(
                    summarySchema->GetSummaryGroupConfig(summaryGroupName)));
}
    
AttributeMergerPtr ParallelEndMergeExecutor::CreateAttributeMerger(
        const MergeTaskItem &item, 
        const IndexPartitionSchemaPtr &schema) const
{
    const string& attrName = item.mName;
    const AttributeSchemaPtr &attrSchema = schema->GetAttributeSchema();
    assert(attrSchema);
    if (item.mMergeType == PACK_ATTRIBUTE_TASK_NAME)
    {
        const PackAttributeConfigPtr& packConfig =
            attrSchema->GetPackAttributeConfig(attrName);
        assert(packConfig);
        AttributeMergerPtr packAttrMerger(
                AttributeMergerFactory::GetInstance()->CreatePackAttributeMerger(
                        packConfig, false, 1024 * 1024));
        return packAttrMerger;
    }
    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(attrName);        
    assert(attrConfig);
    AttributeMergerPtr attrMerger(
            AttributeMergerFactory::GetInstance()->CreateAttributeMerger(
                    attrConfig, false));
    return attrMerger;
}
    
IndexMergerPtr ParallelEndMergeExecutor::CreateIndexMerger(
        const MergeTaskItem &item,
        const IndexPartitionSchemaPtr &schema) const
{
    const IndexSchemaPtr &indexSchema = schema->GetIndexSchema();
    const IndexConfigPtr &indexConfig = indexSchema->GetIndexConfig(item.mName);
    IndexType indexType = indexConfig->GetIndexType();
    IndexMergerPtr indexMerger(
            IndexMergerFactory::GetInstance()->CreateIndexMerger(
                    indexType, mPluginManager));
    index_base::MergeItemHint hint;
    index_base::MergeTaskResourceVector taskResources;
    config::MergeIOConfig ioConfig;
    indexMerger->Init(indexConfig, hint, taskResources, ioConfig, NULL, NULL);
    return indexMerger;
}

vector<MergeTaskResourceVector> ParallelEndMergeExecutor::ExtractMergeTaskResource(
        const MergeTaskResourceManagerPtr& mgr, const MergeTaskItems &taskGroup) const
{
    vector<MergeTaskResourceVector> ret;
    ret.reserve(taskGroup.size());
    for (size_t i = 0; i < taskGroup.size(); i++)
    {
        MergeTaskResourceVector resVec;
        vector<resourceid_t> resourceIds = taskGroup[i].GetParallelMergeItem().GetResourceIds();
        for (auto id : resourceIds)
        {
            MergeTaskResourcePtr resource = mgr->GetResource(id);
            if (!resource)
            {
                INDEXLIB_FATAL_ERROR(InconsistentState, "get merge task resource [id:%d] fail!", id);
            }
            resVec.push_back(resource);
        }
        ret.push_back(resVec);
    }
    return ret;
}
    

IE_NAMESPACE_END(merger);

