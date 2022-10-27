#include "indexlib/merger/custom_partition_merger.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/merge_policy.h"
#include "indexlib/table/table_merge_plan_meta.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/merger/merge_work_item.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/merger/merge_task_executor.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/util/resource_control_work_item.h"
#include "indexlib/util/resource_control_thread_pool.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_base/index_meta/progress_synchronizer.h"
#include "indexlib/index_base/index_meta/segment_info.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, CustomPartitionMerger);

CustomPartitionMerger::CustomPartitionMerger(
    const SegmentDirectoryPtr& segDir,
    const IndexPartitionSchemaPtr& schema,
    const IndexPartitionOptions& options,
    const DumpStrategyPtr& dumpStrategy,
    misc::MetricProviderPtr metricProvider,
    const TableFactoryWrapperPtr& tableFactory,
    const PartitionRange& range)
    : IndexPartitionMerger(segDir, schema, options, dumpStrategy,
                           metricProvider, tableFactory->GetPluginManager(), range)
    , mTableFactory(tableFactory)
{
}

CustomPartitionMerger::~CustomPartitionMerger() 
{
}

MergeMetaPtr CustomPartitionMerger::CreateMergeMeta(bool optimize, uint32_t instanceCount, int64_t currentTs)
{
    MergeTask mergeTask;
    MergePolicyPtr mergePolicy(mTableFactory->CreateMergePolicy());
    if (!mergePolicy.get())
    {
        MergeMetaPtr meta(new TableMergeMeta(mergePolicy));
        MultiPartSegmentDirectoryPtr multiPartSegDir =
            DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, mSegmentDirectory);
        if (multiPartSegDir)
        {
            INDEXLIB_FATAL_ERROR(Runtime, "does not support parallel build"
                                 " when MergePlicy is not implemented");
        }
        IE_LOG(WARN, "MergePolicy is not defined in plugin, "
               "empty MergeMeta will be used.");
        return meta;
    }
    if (!mergePolicy->Init(mSchema, mOptions))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "Init MergePolicy failed");
    }

    TableMergeMetaPtr tableMergeMeta = CreateTableMergeMeta(
        mergePolicy, optimize, instanceCount, currentTs);
    IE_LOG(INFO, "Create TableMergeMeta done, plan count[%zu]", tableMergeMeta->mergePlans.size());
    return tableMergeMeta;
}

MergeMetaPtr CustomPartitionMerger::LoadMergeMeta(
    const string& mergeMetaPath, bool onlyLoadBasicInfo)
{
    MergePolicyPtr mergePolicy(mTableFactory->CreateMergePolicy());
    if (mergePolicy)
    {
        if (!mergePolicy->Init(mSchema, mOptions))
        {
            INDEXLIB_FATAL_ERROR(Runtime, "Init MergePolicy failed");            
        }
    }
    MergeMetaPtr mergeMeta(new TableMergeMeta(mergePolicy));
    if (onlyLoadBasicInfo)
    {
        return mergeMeta->LoadBasicInfo(mergeMetaPath) ? mergeMeta : MergeMetaPtr();
    }
    return mergeMeta->Load(mergeMetaPath) ? mergeMeta : MergeMetaPtr(); 
}

void CustomPartitionMerger::DoMerge(
    bool optimize, const MergeMetaPtr& mergeMeta, uint32_t instanceId)
{
    TableMergeMetaPtr tableMergeMeta = DYNAMIC_POINTER_CAST(TableMergeMeta, mergeMeta);
    if (!tableMergeMeta)
    {
        INDEXLIB_FATAL_ERROR(
            Runtime, "fail to cast MergeMeta to TableMergeMeta");
    }
    if (instanceId >= tableMergeMeta->instanceExecMetas.size())
    {
        IE_LOG(INFO, "nothing to merge for instance[%d].", instanceId); 
        return;
    }
    MergeFileSystemPtr mergeFileSystem = CreateMergeFileSystem(
        mDumpStrategy->GetRootDir(), instanceId, mergeMeta);
    const TaskExecuteMetas& taskExecMetas = tableMergeMeta->instanceExecMetas[instanceId];

    vector<SegmentMetaPtr> segmentMetas;
    segmentMetas.reserve(mSegmentDirectory->GetSegments().size());
    if (!CreateSegmentMetas(mSegmentDirectory, segmentMetas))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "fail to create segment metas for merge");
    }
    
    MergeTaskExecutorPtr taskExecutor(
        new MergeTaskExecutor(
            mSchema, mOptions, mCounterMap, &mMetrics, optimize, mergeFileSystem));

    if (!taskExecutor->Init(
            tableMergeMeta, mTableFactory, segmentMetas))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "Init MergeTaskExecutor failed");
    }
    
    vector<ResourceControlWorkItemPtr> mergeWorkItems;
    uint32_t itemCount = 0;    
    for (const auto& taskExecMeta : taskExecMetas)
    {
        bool errorFlag = false;
        MergeWorkItemPtr mergeItem(taskExecutor->CreateMergeWorkItem(taskExecMeta, errorFlag));
        if (errorFlag)
        {
            INDEXLIB_FATAL_ERROR(Runtime, "create MergeWorkItem failed");            
        }
        
        if (mergeItem.get() == NULL)
        {
            continue;            
        }
        ++itemCount;        
        mergeWorkItems.push_back(mergeItem);
    }
    if (itemCount == 0)
    {
        IE_LOG(WARN, "no merge task item for current instance [%u],"
               " maybe start too many merge instance!", instanceId);
    }    
    mMetrics.StartReport(mCounterMap);    
    int64_t maxMemUse = mMergeConfig.maxMemUseForMerge * 1024 * 1024;
    maxMemUse -= mergeMeta->EstimateMemoryUse();
    MergeSchedulerPtr scheduler = CreateMergeScheduler(maxMemUse, mMergeConfig.mergeThreadCount);
    scheduler->Run(mergeWorkItems, mergeFileSystem);
    mergeFileSystem->Close();
    mMetrics.StopReport();    
}

void CustomPartitionMerger::EndMerge(
    const MergeMetaPtr& mergeMeta, versionid_t alignVersionId)
{
    const Version& targetVersion = mergeMeta->GetTargetVersion(); 
    if (targetVersion == INVALID_VERSION)
    {
        AlignVersion(mDumpStrategy->GetRootDir(), alignVersionId);
        return;
    }
    TableMergeMetaPtr tableMergeMeta =
        DYNAMIC_POINTER_CAST(TableMergeMeta, mergeMeta);
    if (!tableMergeMeta)
    {
        INDEXLIB_FATAL_ERROR(
            Runtime, "fail to cast MergeMeta to TableMergeMeta");
    }
    assert(targetVersion.GetVersionId() != INVALID_VERSION);    
    Version alignVersion = targetVersion;
    alignVersion.SetVersionId(GetTargetVersionId(targetVersion, alignVersionId));
    alignVersion.SyncSchemaVersionId(mSchema);

    const string &rootPath = mDumpStrategy->GetRootDir();

    Version latestOnDiskVersion;
    VersionLoader::GetVersion(rootPath, latestOnDiskVersion, INVALID_VERSION);
    
    if (latestOnDiskVersion == alignVersion)
    {
        // handle failover
        RemoveInstanceDir(rootPath);
        return;
    }

    if (!MergeInstanceDir(rootPath, tableMergeMeta)) {
        INDEXLIB_FATAL_ERROR(Runtime, "fail to mergeInstanceDir to rootPath[%s]", rootPath.c_str());
    }
    VersionCommitter committer(rootPath, alignVersion, mKeepVersionCount,
                               mOptions.GetReservedVersions());
    committer.Commit();
    RemoveInstanceDir(rootPath);
}

bool CustomPartitionMerger::CreateSegmentMetas(
    const SegmentDirectoryPtr& segmentDirectory,
    vector<SegmentMetaPtr>& segMetas) const
{
    PartitionDataPtr partitionData = segmentDirectory->GetPartitionData();

    for (PartitionData::Iterator it = partitionData->Begin();
         it != partitionData->End(); ++it)
    {
        const SegmentData& segmentData = *it;
        SegmentMetaPtr segMeta(new SegmentMeta());
        segMeta->segmentDataBase = SegmentDataBase(segmentData);
        segMeta->segmentInfo = segmentData.GetSegmentInfo();
        segMeta->type = SegmentMeta::SegmentSourceType::INC_BUILD;
        DirectoryPtr segDir = segmentData.GetDirectory();
        DirectoryPtr dataDir = segDir->GetDirectory(CUSTOM_DATA_DIR_NAME, false);
        if (!dataDir)
        {
            IE_LOG(ERROR, "fail to get data dir[%s] in segment[%d]",
                   CUSTOM_DATA_DIR_NAME.c_str(), segmentData.GetSegmentId());
            return false;
        }
        segMeta->segmentDataDirectory = dataDir;
        segMetas.push_back(segMeta);
    }
    return true;
}

TableMergePlanMetaPtr CustomPartitionMerger::CreateTableMergePlanMeta(
    const vector<SegmentMetaPtr>& inPlanSegmentMetas,
    segmentid_t targetSegmentId) const
{
    TableMergePlanMetaPtr tableMergePlanMeta(new TableMergePlanMeta());
    MultiPartSegmentDirectoryPtr multiPartSegDir =
        DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, mSegmentDirectory);
    if (multiPartSegDir)
    {
        vector<SegmentInfo> segInfos = GetLastSegmentInfosForMultiPartitionMerge(
            multiPartSegDir, inPlanSegmentMetas);
        // use maxTs & max locator
        ProgressSynchronizer ps;
        ps.Init(segInfos);
        tableMergePlanMeta->locator = ps.GetLocator();
        tableMergePlanMeta->timestamp = ps.GetTimestamp();
    }
    else
    {
        tableMergePlanMeta->locator = inPlanSegmentMetas.back()->segmentInfo.locator;
        tableMergePlanMeta->timestamp = inPlanSegmentMetas.back()->segmentInfo.timestamp;
    }
    tableMergePlanMeta->targetSegmentId = targetSegmentId;
    return tableMergePlanMeta;
}

vector<SegmentInfo> CustomPartitionMerger::GetLastSegmentInfosForMultiPartitionMerge(
    const MultiPartSegmentDirectoryPtr& multiPartSegDir,
    const vector<SegmentMetaPtr>& segmentMetas) const
{
    vector<SegmentInfo> segInfos;
    vector<segmentid_t> lastSegIds;
    uint32_t partCount = multiPartSegDir->GetPartitionCount();
    lastSegIds.reserve(partCount);
    segInfos.reserve(partCount);
    for (uint32_t partId = 0; partId < partCount; ++partId)
    {
        const std::vector<segmentid_t>&  partSegIds =
            multiPartSegDir->GetSegIdsByPartId(partId);
        if (partSegIds.empty())
        {
            continue;
        }
        lastSegIds.push_back(partSegIds.back());
    }

    for (const auto& segmentMeta : segmentMetas)
    {
        if (find(lastSegIds.begin(), lastSegIds.end(),
                 segmentMeta->segmentDataBase.GetSegmentId()) == lastSegIds.end())
        {
            continue;
        }
        segInfos.push_back(segmentMeta->segmentInfo);
    }
    return segInfos;
}

Version CustomPartitionMerger::CreateNewVersion(
    const vector<TableMergePlanPtr>& mergePlans,
    const vector<TableMergePlanMetaPtr>& mergePlanMetas)
{
    for (size_t i = 0; i < mergePlans.size(); ++i)
    {
        const auto& inPlanSegAttrs = mergePlans[i]->GetInPlanSegments();
        for (const auto& segAttr : inPlanSegAttrs)
        {
            if (!segAttr.second)
            {
                mDumpStrategy->RemoveSegment(segAttr.first);
            }
        }
    }
    for (size_t i = 0; i < mergePlans.size(); ++i)
    {
        mDumpStrategy->AddMergedSegment(mergePlanMetas[i]->targetSegmentId);
    }
    mDumpStrategy->IncVersionId();
    return mDumpStrategy->IsDirty() ? mDumpStrategy->GetVersion() : Version(INVALID_VERSION);
}

bool CustomPartitionMerger::FilterEmptyTableMergePlan(vector<TableMergePlanPtr>& tableMergePlans)
{
    size_t planIdx = 0;
    size_t beforeFilterSize = tableMergePlans.size();
    for (auto it = tableMergePlans.begin(); it != tableMergePlans.end(); )
    {
        const TableMergePlanPtr& tableMergePlan = *it;
        if (tableMergePlan->GetInPlanSegments().size() == 0)
        {
            IE_LOG(WARN, "TableMergePlan[%zu] has no segments, "
                   "this plan will be filtered", planIdx);

            it = tableMergePlans.erase(it);
        }
        else
        {
            ++it;
        }
        planIdx++;
    }
    return tableMergePlans.size() != beforeFilterSize;
}

TableMergeMetaPtr CustomPartitionMerger::CreateTableMergeMeta(
    const MergePolicyPtr& mergePolicy, bool isFullMerge,
    uint32_t instanceCount, int64_t currentTs)
{
    TableMergeMetaPtr tableMergeMeta(new TableMergeMeta(mergePolicy));
    vector<SegmentMetaPtr> segmentMetas;
    segmentMetas.reserve(mSegmentDirectory->GetSegments().size());
    if (!CreateSegmentMetas(mSegmentDirectory, segmentMetas))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "fail to create segment metas for merge");
    }
    const MergeConfig& mergeConfig = mOptions.GetOfflineConfig().mergeConfig;

    vector<TableMergePlanPtr> mergePlans;
    if (isFullMerge)
    {
        mergePlans = mergePolicy->CreateMergePlansForFullMerge(
            mergeConfig.mergeStrategyStr, mergeConfig.mergeStrategyParameter, segmentMetas, mPartitionRange);
    }
    else
    {
        mergePlans = mergePolicy->CreateMergePlansForIncrementalMerge(
            mergeConfig.mergeStrategyStr, mergeConfig.mergeStrategyParameter, segmentMetas, mPartitionRange);
    }
    IE_LOG(INFO, "create TableMergePlan done, planCount[%zu]", mergePlans.size());

    if (FilterEmptyTableMergePlan(mergePlans))
    {
        IE_LOG(WARN, "after filter empty merge plans, planCount is [%zu]", mergePlans.size());        
    }
    
    vector<TableMergePlanResourcePtr> planResources;
    planResources.reserve(mergePlans.size());
        
    for (size_t i = 0; i < mergePlans.size(); ++i)
    {
        TableMergePlanResourcePtr resource(mergePolicy->CreateMergePlanResource());
        if (resource)
        {
            if (!resource->Init(mergePlans[i], segmentMetas))
            {
                INDEXLIB_FATAL_ERROR(Runtime,
                                     "fail to Init Resource for MergePlan[%zu] in schema[%s]",
                                     i, mSchema->GetSchemaName().c_str());
            }
        }
        planResources.push_back(resource);
    }

    vector<MergeTaskDescriptions> taskDescriptionsVec;
    taskDescriptionsVec.reserve(mergePlans.size());

    vector<TableMergePlanMetaPtr> mergePlanMetas;
    mergePlanMetas.reserve(mergePlans.size());

    vector<vector<SegmentMetaPtr>> inPlanSegmentMetas;
    inPlanSegmentMetas.resize(mergePlans.size());

    segmentid_t lastSegmentId = mDumpStrategy->GetLastSegmentId();
    for (size_t i = 0; i < mergePlans.size(); ++i)
    {
        CreateInPlanSegmentMetas(segmentMetas, mergePlans[i], inPlanSegmentMetas[i]);
        
        TableMergePlanMetaPtr planMeta = CreateTableMergePlanMeta(
            inPlanSegmentMetas[i], lastSegmentId + 1 + (int32_t)i);
        mergePlanMetas.push_back(planMeta);
    }

    for (size_t planIdx = 0; planIdx < mergePlans.size(); ++planIdx)
    {
        MergeSegmentDescription segDescription;
        MergeTaskDescriptions tasks = mergePolicy->CreateMergeTaskDescriptions(
            mergePlans[planIdx], planResources[planIdx], inPlanSegmentMetas[planIdx], segDescription);
        if (!ValidateMergeTaskDescriptions(tasks)) {
            INDEXLIB_FATAL_ERROR(Runtime, "fail to prepare MergeTaskDescriptions"
                                 " for MergePlan[%zu], in schema[%s]",
                                 planIdx, mSchema->GetSchemaName().c_str()); 
        }
        taskDescriptionsVec.push_back(tasks);
        if (segDescription.docCount < 0)
        {
            INDEXLIB_FATAL_ERROR(Runtime, "invalid docCount[%ld] for MergePlan[%zu]",
                                 segDescription.docCount, planIdx);
        }
        mergePlanMetas[planIdx]->segmentDescription = segDescription;
    }

    MergeTaskDispatcherPtr dispatcher = mergePolicy->CreateMergeTaskDispatcher();
    tableMergeMeta->instanceExecMetas =
        dispatcher->DispatchMergeTasks(taskDescriptionsVec, instanceCount);

    tableMergeMeta->SetTargetVersion(CreateNewVersion(mergePlans, mergePlanMetas)); 
    tableMergeMeta->mergePlans = mergePlans;
    tableMergeMeta->mergePlanMetas = mergePlanMetas;
    tableMergeMeta->mergePlanResources = planResources;
    tableMergeMeta->mergeTaskDescriptions = taskDescriptionsVec;
    return tableMergeMeta;
}

bool CustomPartitionMerger::ValidateMergeTaskDescriptions(
    const MergeTaskDescriptions& taskDescriptions)
{
    if (taskDescriptions.size() == 0)
    {
        IE_LOG(ERROR, "mergeTask should not be empty");
        return false;
    }
    for (size_t i = 0; i < taskDescriptions.size(); ++i)
    {
        // check for uniqueness of task name
        for (size_t j = i+1; j < taskDescriptions.size(); ++j)
        {
            if (taskDescriptions[i].name == taskDescriptions[j].name)
            {
                IE_LOG(ERROR, "task[%zu] and task[%zu] should not share the same name[%s]",
                       i, j, taskDescriptions[i].name.c_str());
                return false;
            }
        }
        if (taskDescriptions[i].cost < 0)
        {
            IE_LOG(ERROR, "task[%zu] should not be assigned to a negative cost[%u]",
                   i, taskDescriptions[i].cost);
            return false;
        }
    }
    return true;
}

bool CustomPartitionMerger::MergeInstanceDir(
    const string& rootPath, const TableMergeMetaPtr& mergeMeta)
{
    assert(mergeMeta);
    IndexlibFileSystemPtr fs = IndexlibFileSystemCreator::Create(rootPath);
    
    fslib::FileList instanceDirList = ListInstanceDirs(rootPath);
    vector<vector<string>> mergeTaskPaths;
    // TODO: mergeTaskPaths become to <Directory>
    if (!CollectMergeTaskPaths(instanceDirList, mergeMeta, mergeTaskPaths))
    {
        IE_LOG(ERROR, "collect merge task paths from [%s] failed",
               rootPath.c_str());
        return false;
    }
    vector<vector<DirectoryPtr>> inputDirectorys;
    inputDirectorys.resize(mergeTaskPaths.size());
    for (size_t i = 0; i < mergeTaskPaths.size(); ++i)
    {
        for (size_t j = 0; j < mergeTaskPaths[i].size(); ++j)
        {
            inputDirectorys[i].push_back(DirectoryCreator::Get(fs, mergeTaskPaths[i][j], true));
        }
    }

    //MergePolicyPtr mergePolicy(mTableFactory->CreateMergePolicy());
    MergePolicyPtr mergePolicy = mergeMeta->GetMergePolicy();
    if (!mergePolicy.get())
    {
        IE_LOG(ERROR, "cannot mergeInstanceDir "
               "when MergePolicy is not implemented in TableFactory");
        return false;
    }

    if (!mergePolicy->Init(mSchema, mOptions))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "Init MergePolicy failed");
    }
    
    for (size_t planIdx = 0; planIdx < mergeMeta->GetMergePlanCount(); ++planIdx)
    {
        segmentid_t targetSegId = mergeMeta->mergePlanMetas[planIdx]->targetSegmentId;
        string segDirName = mergeMeta->GetTargetVersion().GetSegmentDirName(targetSegId);
        string targetSegDir = storage::FileSystemWrapper::JoinPath(rootPath, segDirName);
        // handle failover
        bool isFailOver = storage::FileSystemWrapper::IsExist(targetSegDir);
        if (!isFailOver)
        {
            storage::FileSystemWrapper::MkDir(targetSegDir);
        }
        string segDataPath = storage::FileSystemWrapper::JoinPath(targetSegDir, CUSTOM_DATA_DIR_NAME);
        storage::FileSystemWrapper::MkDirIfNotExist(segDataPath);
        DirectoryPtr outputDirectory = DirectoryCreator::Get(fs, segDataPath, true);
        if (!mergePolicy->ReduceMergeTasks(mergeMeta->mergePlans[planIdx],
                        mergeMeta->mergeTaskDescriptions[planIdx],
                        inputDirectorys[planIdx], outputDirectory, isFailOver))
        {
            IE_LOG(ERROR, "reduce MergeTasks for mergePlan[%zu] to path[%s] failed",
                   planIdx, segDataPath.c_str());
            return false;
        }
        const TableMergePlanMetaPtr& mergePlanMeta = mergeMeta->mergePlanMetas[planIdx];
        SegmentInfo segmentInfo = CreateSegmentInfo(mergePlanMeta);
        segmentInfo.StoreIgnoreExist(
            storage::FileSystemWrapper::JoinPath(targetSegDir, SEGMENT_INFO_FILE_NAME));

        const auto& segDescription = mergePlanMeta->segmentDescription;
        if (segDescription.useSpecifiedDeployFileList)
        {
            fslib::FileList customFileList;
            customFileList.reserve(segDescription.deployFileList.size() + 1);
            for (const auto& file: segDescription.deployFileList)
            {
                customFileList.push_back(
                    storage::FileSystemWrapper::JoinPath(CUSTOM_DATA_DIR_NAME, file));
            }
            customFileList.push_back(
                storage::FileSystemWrapper::NormalizeDir(CUSTOM_DATA_DIR_NAME));
            customFileList.push_back(SEGMENT_INFO_FILE_NAME);
            DeployIndexWrapper::DumpSegmentDeployIndex(
                targetSegDir, customFileList);
        }
        else
        {
            DeployIndexWrapper dpIndexWrapper(rootPath);
            if (!dpIndexWrapper.DumpSegmentDeployIndex(segDirName))
            {
                IE_LOG(ERROR, "dump segement deploy index failed, "
                       "rootPath [%s], targetSeg [%s]",
                       rootPath.c_str(), targetSegDir.c_str());
                return false;
            }
        }
    }
    return true;
}

void CustomPartitionMerger::RemoveInstanceDir(
    const string& rootPath) const
{
    fslib::FileList instanceDirList = ListInstanceDirs(rootPath);
    for (size_t i = 0; i < instanceDirList.size(); i++)
    {
        storage::FileSystemWrapper::DeleteIfExist(instanceDirList[i]);
    }    
}

bool CustomPartitionMerger::CollectMergeTaskPaths(
    const fslib::FileList& instanceDirList,
    const TableMergeMetaPtr& mergeMeta,
    vector<vector<string>>& mergeTaskPaths)
{
    assert(mergeMeta);
    if (instanceDirList.size() != mergeMeta->instanceExecMetas.size())
    {
        IE_LOG(ERROR, "instance execute meta size[%zu] "
               "does not match instance dir count [%zu]",
               mergeMeta->instanceExecMetas.size(), instanceDirList.size());
        return false;
    }
    size_t planCount = mergeMeta->GetMergePlanCount();
    const Version& targetVersion = mergeMeta->GetTargetVersion();
    mergeTaskPaths.reserve(planCount);

    for (size_t planIdx = 0; planIdx < planCount; ++planIdx)
    {
        size_t taskCountInPlan = mergeMeta->mergeTaskDescriptions[planIdx].size();
        vector<string> taskPathsForOnePlan(taskCountInPlan, "");
        mergeTaskPaths.push_back(taskPathsForOnePlan);
    }

    for (const auto& instancePath : instanceDirList)
    {
        for (size_t planIdx = 0; planIdx < planCount; ++planIdx)
        {
            assert(mergeMeta->GetTargetSegmentIds(planIdx).size() == 1u);
            string targetSegName
                = targetVersion.GetSegmentDirName(mergeMeta->GetTargetSegmentIds(planIdx)[0]);
            string segPath = storage::FileSystemWrapper::JoinPath(instancePath, targetSegName);
            fslib::FileList taskDirNames;
            storage::FileSystemWrapper::ListDir(segPath, taskDirNames);
            for (const auto& taskDirName : taskDirNames)
            {
                uint32_t planIdx, taskIdx;
                if (TaskExecuteMeta::GetIdxFromIdentifier(taskDirName, planIdx, taskIdx))
                {
                    mergeTaskPaths[planIdx][taskIdx] =
                        storage::FileSystemWrapper::JoinPath(segPath, taskDirName);
                }
            }
        }
    }
    // verify mergeTaskPaths
    for (size_t planIdx = 0; planIdx < planCount; ++planIdx)
    {
        const vector<string>& planTasks = mergeTaskPaths[planIdx];
        for (size_t taskIdx = 0; taskIdx < planTasks.size(); ++taskIdx)
        {
            if (planTasks[taskIdx].empty())
            {
                IE_LOG(ERROR, "fail to get merge task path for "
                       "MergePlan[%zu], TaskIdx[%zu]", planIdx, taskIdx)
            }
        }
    }
    return true;
}

SegmentInfo CustomPartitionMerger::CreateSegmentInfo(
        const table::TableMergePlanMetaPtr& mergePlanMeta) const
{
    SegmentInfo segInfo;
    segInfo.docCount = mergePlanMeta->segmentDescription.docCount;
    segInfo.locator = mergePlanMeta->locator;
    segInfo.timestamp = mergePlanMeta->timestamp;
    segInfo.mergedSegment = true;
    segInfo.maxTTL = mergePlanMeta->maxTTL;
    return segInfo;
}


void CustomPartitionMerger::CreateInPlanSegmentMetas(
    const vector<SegmentMetaPtr> allSegMetas,
    const TableMergePlanPtr& mergePlan,
    vector<SegmentMetaPtr>& inPlanSegmentMetas)
{
    const auto& inPlanSegAttrs = mergePlan->GetInPlanSegments();
    inPlanSegmentMetas.reserve(inPlanSegAttrs.size());
    
    for (const auto& segMeta : allSegMetas)
    {
        segmentid_t segId = segMeta->segmentDataBase.GetSegmentId();
        if (inPlanSegAttrs.find(segId) != inPlanSegAttrs.end())
        {
            inPlanSegmentMetas.push_back(segMeta);
        }
    }
}

IE_NAMESPACE_END(merger);

