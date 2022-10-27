#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/index/multi_segment_metrics_updater.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_factory.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_factory.h"
#include "indexlib/merger/parallel_end_merge_executor.h"
#include "indexlib/merger/merge_work_item_typed.h"
#include "indexlib/merger/multi_threaded_merge_scheduler.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer_creator.h"
#include "indexlib/merger/merge_work_item_creator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/config/truncate_index_name_mapper.h"
#include "indexlib/merger/merge_task_item_dispatcher.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/util/resource_control_thread_pool.h"
#include "indexlib/common/chunk/chunk_decoder_creator.h"
#include "indexlib/merger/document_reclaimer/document_reclaimer.h"
#include "indexlib/merger/merge_meta_creator_factory.h"
#include "indexlib/merger/split_strategy/split_segment_strategy_factory.h"
#include "indexlib/merger/merge_meta_work_item.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map_creator.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/file_system/directory_merger.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/plugin/plugin_factory_loader.h"
#include <beeper/beeper.h>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(merger);

IE_LOG_SETUP(merger, IndexPartitionMerger);

const string IndexPartitionMerger::MERGE_INSTANCE_DIR_PREFIX = "instance_";
const string IndexPartitionMerger::INSTANCE_TARGET_VERSION_FILE = "target_version";

IndexPartitionMerger::IndexPartitionMerger(const SegmentDirectoryPtr& segDir,
                                           const IndexPartitionSchemaPtr& schema,
                                           const IndexPartitionOptions& options,
                                           const DumpStrategyPtr &dumpStrategy,
                                           misc::MetricProviderPtr metricProvider,
                                           const plugin::PluginManagerPtr& pluginManager,
                                           const PartitionRange& range)
    : mSegmentDirectory(segDir)
    , mSchema(schema)
    , mOptions(options)
    , mMergeConfig(options.GetMergeConfig())
    , mKeepVersionCount(options.GetBuildConfig().keepVersionCount)
    , mDumpStrategy(dumpStrategy)
    , mMetrics(metricProvider)
    , mCounterMap(new CounterMap())
    , mPluginManager(pluginManager)
    , mMetricProvider(metricProvider)
    , mPartitionRange(range)
{
    assert(segDir);
    if (mDumpStrategy == NULL)
    {
        mDumpStrategy.reset(new DumpStrategy(segDir));
    }
    FileSystemWrapper::SetMergeIOConfig(mMergeConfig.mergeIOConfig);
    ChunkDecoderCreator::Reset();

    mSubPartSegmentDirectory = mSegmentDirectory->GetSubSegmentDirectory();
    if (mPluginManager)
    {
        plugin::PluginResourcePtr pluginResource = mPluginManager->GetPluginResource();
        assert(pluginResource);
        pluginResource->counterMap = mCounterMap;
    }
}

IndexPartitionMerger::IndexPartitionMerger()
{
}

IndexPartitionMerger::~IndexPartitionMerger()
{
}

bool IndexPartitionMerger::Merge(bool optimize, int64_t currentTs)
{
    PrepareMerge(currentTs);
    Version tmpVersion = mDumpStrategy->getDumpingVersion();
    MergeMetaPtr meta = CreateMergeMeta(optimize, 1, currentTs);
    CleanTargetDirs(meta);
    DoMerge(optimize, meta, 0);

    // for test logic
    char* envStr = getenv("INDEXLIB_TEST_MERGE_CHECK_POINT");
    if (envStr && std::string(envStr) == "true")
    {
        mDumpStrategy->RevertVersion(tmpVersion);
        PrepareMerge(currentTs);
        MergeMetaPtr meta = CreateMergeMeta(optimize, 1, currentTs);
        TEST_MERGE_WITH_CHECKPOINTS(optimize, meta, 0);
    }

    EndMerge(meta);
    return meta->GetMergePlanCount() != 0;
}

void IndexPartitionMerger::CleanTargetDirs(const MergeMetaPtr& mergeMeta) {
    const Version& targetVersion = mergeMeta->GetTargetVersion();
    size_t planCount = mergeMeta->GetMergePlanCount();
    for (size_t i = 0; i < planCount; i++)
    {
        auto targetSegmentIds = mergeMeta->GetTargetSegmentIds(i);
        for (const segmentid_t segId : targetSegmentIds)
        {
            auto targetSegment = targetVersion.GetSegmentDirName(segId);
            string partitionDir = mDumpStrategy->GetRootDir();
            string targetSegmentPath = PathUtil::JoinPath(partitionDir, targetSegment);
            FileSystemWrapper::DeleteIfExist(targetSegmentPath);
        }
        // no need to delete version file, commit version will remove old first
    }
}

IndexMergeMeta* IndexPartitionMerger::CreateMergeMetaWithTask(MergeTask &task,
        const SegmentMergeInfos &segMergeInfos,
        bool optimize, uint32_t instanceCount)
{
    const auto& updaterConfig = mOptions.GetBuildConfig().GetSegmentMetricsUpdaterConfig();
    SplitSegmentStrategyFactory* splitStrategyFactory = nullptr;
    const auto& splitConfig = mMergeConfig.GetSplitSegmentConfig();
    SplitSegmentStrategyFactory factory;
    if (plugin::PluginManager::isBuildInModule(splitConfig.moduleName))
    {
        splitStrategyFactory = &factory;
    }
    else
    {
        splitStrategyFactory = plugin::PluginFactoryLoader::GetFactory<SplitSegmentStrategyFactory,
            SplitSegmentStrategyFactory>(splitConfig.moduleName,
            SplitSegmentStrategyFactory::SPLIT_SEGMENT_STRATEGY_FACTORY_SUFFIX, mPluginManager);
    }
    if (!splitStrategyFactory)
    {
        INDEXLIB_FATAL_ERROR(Runtime, "get split segment strategy factory from moudle [%s] failed",
            splitConfig.moduleName.c_str());
    }
    auto generateUpdater = [segMergeInfos, updaterConfig, this]() mutable {
        std::unique_ptr<MultiSegmentMetricsUpdater> updater(new MultiSegmentMetricsUpdater);
        updater->InitForMerge(
            mSchema, mOptions, segMergeInfos, mAttrReaderContainer, updaterConfig, mPluginManager);
        return updater;
    };
    splitStrategyFactory->Init(mSegmentDirectory, mSchema, mAttrReaderContainer, segMergeInfos,
        std::move(generateUpdater));

    MergeMetaCreatorPtr mergeMetaCreator
        = MergeMetaCreatorFactory::Create(mSchema, CreateReclaimMapCreator(), splitStrategyFactory);
    mergeMetaCreator->Init(mSegmentDirectory, mMergeConfig, mDumpStrategy,
                           mPluginManager, instanceCount);
    return mergeMetaCreator->Create(task);
}

MergeMetaPtr IndexPartitionMerger::CreateMergeMeta(
        bool optimize, uint32_t instanceCount, int64_t currentTs)
{
    mAttrReaderContainer.reset(
        new OfflineAttributeSegmentReaderContainer(mSchema, mSegmentDirectory));
    mSubPartSegmentDirectory = mSegmentDirectory->GetSubSegmentDirectory();
    SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos(mSegmentDirectory);
    IE_LOG(INFO, "create segmentMergeInfos done");
    if (segMergeInfos.size() == 0)
    {
        // nothing to merge
        return MergeMetaPtr(new IndexMergeMeta());
    }

    LevelInfo levelInfo = mSegmentDirectory->GetVersion().GetLevelInfo();
    MergeTask task = CreateMergeTaskByStrategy(optimize, mMergeConfig, segMergeInfos, levelInfo);
    IE_LOG(INFO, "create MergeTask done");
    IndexMergeMeta* meta = CreateMergeMetaWithTask(task, segMergeInfos, optimize, instanceCount);
    IE_LOG(INFO, "create MergeMetaWithTask done");
    meta->SetTimestamp(currentTs);
    try
    {
        meta->SetCounterMap(mSegmentDirectory->GetLatestCounterMapContent());
    }
    catch (...)
    {
        DELETE_AND_SET_NULL(meta);
        throw;
    }
    IE_LOG(INFO, "end create mergeMeta");
    return MergeMetaPtr(meta);
}

MergeMetaPtr IndexPartitionMerger::LoadMergeMeta(
    const string& mergeMetaPath, bool onlyLoadBasicInfo)
{
    MergeMetaPtr mergeMeta(new IndexMergeMeta());
    if (onlyLoadBasicInfo)
    {
        return mergeMeta->LoadBasicInfo(mergeMetaPath) ? mergeMeta : MergeMetaPtr();
    }
    return mergeMeta->Load(mergeMetaPath) ? mergeMeta : MergeMetaPtr();
}

void IndexPartitionMerger::DoMerge(
        bool optimize, const MergeMetaPtr &mergeMeta, uint32_t instanceId)
{
    IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);
    if (instanceId >= indexMergeMeta->GetMergeTaskItemsSize())
    {
        IE_LOG(INFO, "nothing to merge for instance[%d].", instanceId);
        return;
    }
    MergeFileSystemPtr mergeFileSystem = CreateMergeFileSystem(
        mDumpStrategy->GetRootDir(), instanceId, mergeMeta);
    IE_LOG(INFO, "prepare MergeDir done");
    const MergeTaskItems &mergeTaskItems = indexMergeMeta->GetMergeTaskItems(instanceId);
    MergeWorkItemCreatorPtr creator = CreateMergeWorkItemCreator(
        optimize, *indexMergeMeta, mergeFileSystem);

    vector<ResourceControlWorkItemPtr> mergeWorkItems;
    uint32_t itemCount = 0;
    for (MergeTaskItems::const_iterator it = mergeTaskItems.begin();
         it != mergeTaskItems.end(); ++it)
    {
        MergeWorkItemPtr mergeItem(creator->CreateMergeWorkItem(*it));
        if (mergeItem.get() == NULL)
        {
            continue;
        }
        mergeWorkItems.push_back(mergeItem);
        ++itemCount;
    }

    if (itemCount == 0)
    {
        IE_LOG(WARN, "no merge task item for current instance [%u],"
               " maybe start too many merge instance!", instanceId);
    }
    mMetrics.StartReport(mCounterMap);
    IE_LOG(INFO, "create MergeWorkItem done");
    int64_t maxMemUse = mMergeConfig.maxMemUseForMerge * 1024 * 1024;
    maxMemUse -= mergeMeta->EstimateMemoryUse(); // reclaim map and bucket map
    MergeSchedulerPtr scheduler = CreateMergeScheduler(maxMemUse, mMergeConfig.mergeThreadCount);
    scheduler->Run(mergeWorkItems, mergeFileSystem);
    mergeFileSystem->Close();
    mMetrics.StopReport();
    creator.reset();
}

void IndexPartitionMerger::PrepareMergeDir(const string &rootDir, uint32_t instanceId,
        const MergeMetaPtr &mergeMeta, string &instanceRootPath)
{
    instanceRootPath = GetInstanceTempMergeDir(rootDir, instanceId);
    string instTargetVersion = FileSystemWrapper::JoinPath(
            instanceRootPath, INSTANCE_TARGET_VERSION_FILE);
    if (!mMergeConfig.enableMergeItemCheckPoint)
    {
        FileSystemWrapper::DeleteIfExist(instTargetVersion);
    }
    const Version& targetVersion = mergeMeta->GetTargetVersion();
    Version instVersion;
    if (!instVersion.Load(instTargetVersion))
    {
        IE_LOG(INFO, "target_version in instance path [%s] not exist!",
               instanceRootPath.c_str());
        FileSystemWrapper::DeleteIfExist(instanceRootPath);
        FileSystemWrapper::MkDir(instanceRootPath);
    }
    else if (instVersion != targetVersion)
    {
        IE_LOG(INFO, "target_version in path [%s] not match with mergeMeta!",
               instanceRootPath.c_str());
        FileSystemWrapper::DeleteIfExist(instanceRootPath);
        FileSystemWrapper::MkDir(instanceRootPath);
    }

    FileSystemWrapper::AtomicStoreIgnoreExist(instTargetVersion, targetVersion.ToString());
}

string IndexPartitionMerger::GetInstanceTempMergeDir(
        const string &rootDir, uint32_t instanceId)
{
    string curInstanceDir = MERGE_INSTANCE_DIR_PREFIX
                            + StringUtil::toString(instanceId);
    return FileSystemWrapper::JoinPath(rootDir, curInstanceDir);
}

MergeWorkItemCreatorPtr IndexPartitionMerger::CreateMergeWorkItemCreator(
    bool optimize, const IndexMergeMeta& mergeMeta, const MergeFileSystemPtr& mergeFileSystem)
{
    return MergeWorkItemCreatorPtr(new MergeWorkItemCreator(
                    mSchema, mMergeConfig, &mergeMeta, mSegmentDirectory,
                    mSubPartSegmentDirectory, IsSortMerge(), optimize,
                    &mMetrics, mCounterMap, mOptions, mergeFileSystem, mPluginManager));
}

MergeFileSystemPtr IndexPartitionMerger::CreateMergeFileSystem(
    const string& rootPath, uint32_t instanceId, const MergeMetaPtr& mergeMeta)
{
    string instanceRootPath;
    PrepareMergeDir(rootPath, instanceId, mergeMeta, instanceRootPath);

    bool hasSub = mSchema->GetSubIndexPartitionSchema();
    vector<string> targetSegmentPaths;
    const index_base::Version& targetVersion = mergeMeta->GetTargetVersion();
    for (size_t planIdx = 0; planIdx < mergeMeta->GetMergePlanCount(); ++planIdx)
    {
        const std::vector<segmentid_t>& targetSegmentIds = mergeMeta->GetTargetSegmentIds(planIdx);
        for (segmentid_t segmentId : targetSegmentIds)
        {
            string targetSegName = targetVersion.GetSegmentDirName(segmentId);
            targetSegmentPaths.push_back(targetSegName);
            if (hasSub)
            {
                targetSegmentPaths.push_back(
                    PathUtil::JoinPath(targetSegName, SUB_SEGMENT_DIR_NAME));
            }
        }
    }

    MergeFileSystemPtr mergeFileSystem = MergeFileSystem::Create(
        instanceRootPath, mMergeConfig, instanceId, mOptions.GetOfflineConfig().GetRaidConfig());
    mergeFileSystem->Init(targetSegmentPaths);
    return mergeFileSystem;
}

void IndexPartitionMerger::EndMerge(const MergeMetaPtr& mergeMeta,
                                    versionid_t alignVersionId)
{
    IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);
    const vector<MergePlan> &mergePlans = indexMergeMeta->GetMergePlans();
    const Version& targetVersion = indexMergeMeta->GetTargetVersion();

    if (targetVersion == INVALID_VERSION)
    {
        AlignVersion(mDumpStrategy->GetRootDir(), alignVersionId);
        return;
    }
    assert(targetVersion.GetVersionId() != INVALID_VERSION);
    Version alignVersion = targetVersion;
    alignVersion.SetVersionId(GetTargetVersionId(targetVersion, alignVersionId));
    alignVersion.SyncSchemaVersionId(mSchema);

    if (IsTargetVersionValid(alignVersion.GetVersionId()))
    {
        // target version is generated, not need to do end merge again
        return;
    }
    string truncateRoot = mDumpStrategy->GetRootDir();
    string truncateMetaDir = FileSystemWrapper::JoinPath(
            truncateRoot, TRUNCATE_META_DIR_NAME);
    if (FileSystemWrapper::IsExist(truncateMetaDir))
    {
        TruncateIndexNameMapper truncateIndexMapper(truncateMetaDir);
        truncateIndexMapper.Dump(mSchema->GetIndexSchema());
    }

    const string &rootPath = mDumpStrategy->GetRootDir();
    const DirectoryPtr& rootDirectory = DirectoryCreator::Create(rootPath, false, true, true);
    assert(rootDirectory && !rootDirectory->GetPath().empty());

    // merge instance merge dir to temp merge dir
    bool hasSub = (mSubPartSegmentDirectory != NULL);
    MergeInstanceDir(rootDirectory, mergePlans, targetVersion,
                     indexMergeMeta->GetCounterMapContent(), hasSub);
    ParallelEndMerge(rootDirectory, indexMergeMeta);
    SinglePartEndMerge(rootPath, mergePlans, alignVersion);
}

SegmentMetrics IndexPartitionMerger::CollectSegmentMetrics(
    const DirectoryPtr& targetSegmentDir, const MergePlan& plan, size_t targetSegIdx)
{
    // TODO: list all field dir, merge these metrics into one
    SegmentMetrics segMetrics;
    segMetrics.SetSegmentGroupMetrics(
            SEGMENT_CUSTOMIZE_METRICS_GROUP, plan.GetTargetSegmentCustomizeMetrics(targetSegIdx));
    return segMetrics;
}

void IndexPartitionMerger::MergeInstanceDir(
        const DirectoryPtr& rootDirectory,
        const vector<MergePlan> &mergePlans,
        const Version &targetVersion,
        const string& counterMapContent,
        bool hasSub)
{
    const string& rootPath = rootDirectory->GetPath();
    FileList instanceDirList = ListInstanceDirs(rootPath);

    for (size_t j = 0; j < instanceDirList.size(); ++j)
    {
        FileList adaptiveBitmapFiles
            = ListInstanceFiles(instanceDirList[j], ADAPTIVE_DICT_DIR_NAME);
        if (!adaptiveBitmapFiles.empty())
        {
            string adaptiveBitmapDir
                = FileSystemWrapper::JoinPath(rootPath, ADAPTIVE_DICT_DIR_NAME);
            MkDirIfNotExist(adaptiveBitmapDir);
            MoveFiles(adaptiveBitmapFiles, adaptiveBitmapDir);
        }
    }

    for (size_t i = 0; i < mergePlans.size(); i++)
    {
        const auto& plan = mergePlans[i];
        size_t targetSegmentCount = plan.GetTargetSegmentCount();
        for (size_t segIdx = 0; segIdx < targetSegmentCount; ++segIdx)
        {
            segmentid_t targetSegmentId = plan.GetTargetSegmentId(segIdx);
            string segDirName = targetVersion.GetSegmentDirName(targetSegmentId);
            string targetSegmentPath = FileSystemWrapper::JoinPath(rootPath, segDirName);
            MkDirIfNotExist(targetSegmentPath);
            for (size_t j = 0; j < instanceDirList.size(); ++j)
            {
                FileList segmentFiles = ListInstanceFiles(instanceDirList[j], segDirName);
                MoveFiles(segmentFiles, targetSegmentPath);
            }
            if (DirectoryMerger::MergePackageFiles(targetSegmentPath))
            {
                rootDirectory->MountPackageFile(PathUtil::JoinPath(segDirName, PACKAGE_FILE_PREFIX));
            }
            if (hasSub)
            {
                string targetSubSegmentPath = PathUtil::JoinPath(targetSegmentPath, SUB_SEGMENT_DIR_NAME);
                if (DirectoryMerger::MergePackageFiles(targetSubSegmentPath))
                {
                    rootDirectory->MountPackageFile(
                            PathUtil::JoinPath(segDirName, SUB_SEGMENT_DIR_NAME, PACKAGE_FILE_PREFIX));
                }
            }
            DirectoryPtr targetSegmentDir = rootDirectory->GetDirectory(segDirName, true);
            auto segMetrics = CollectSegmentMetrics(targetSegmentDir, plan, segIdx);
            StoreSegmentInfos(plan, segIdx, targetSegmentPath, hasSub, counterMapContent, segMetrics);
        }
    }

    for (size_t i = 0; i < instanceDirList.size(); i++)
    {
        FileSystemWrapper::DeleteDir(instanceDirList[i]);
    }
}

ReclaimMapCreatorPtr IndexPartitionMerger::CreateReclaimMapCreator()
{
    return ReclaimMapCreatorPtr(new ReclaimMapCreator(mMergeConfig, mAttrReaderContainer));
}

void IndexPartitionMerger::StoreSegmentInfos(
        const MergePlan &mergePlan,
        size_t targetSegIdx,
        const string &mergedDir, bool hasSub,
        const string& counterMapContent,
        const SegmentMetrics& segmentMetrics)
{
    segmentMetrics.Store(mergedDir);
    uint64_t keyCount;
    const SegmentTopologyInfo& topoLogoInfo =
       mergePlan.GetTargetTopologyInfo(targetSegIdx);
    string groupName = SegmentWriter::GetMetricsGroupName(
            topoLogoInfo.columnIdx);

    string keyMetricsName = "key_count";
    segmentid_t targetSegmentId = mergePlan.GetTargetSegmentId(targetSegIdx);
    string counterFileName = FileSystemWrapper::JoinPath(mergedDir, COUNTER_FILE_NAME);
    FileSystemWrapper::AtomicStoreIgnoreExist(counterFileName, counterMapContent);

    if (segmentMetrics.Get(groupName, keyMetricsName, keyCount))
    {
        SegmentInfo segmentInfo = mergePlan.GetTargetSegmentInfo(targetSegIdx);
        segmentInfo.docCount = keyCount;
        segmentInfo.schemaId = mSchema->GetSchemaVersionId();
        segmentInfo.StoreIgnoreExist(
                FileSystemWrapper::JoinPath(mergedDir, SEGMENT_INFO_FILE_NAME));
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "EndMerge segment %d, segment info : %s", targetSegmentId,
                segmentInfo.ToString(true).c_str());
        return;
    }

    mergePlan.GetTargetSegmentInfo(targetSegIdx).StoreIgnoreExist(
            FileSystemWrapper::JoinPath(mergedDir, SEGMENT_INFO_FILE_NAME));
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
            "EndMerge segment %d, segment info : %s", targetSegmentId,
            mergePlan.GetTargetSegmentInfo(targetSegIdx).ToString(true).c_str());
    if (hasSub)
    {
        counterFileName = PathUtil::JoinPath(mergedDir,
                SUB_SEGMENT_DIR_NAME, COUNTER_FILE_NAME);
        FileSystemWrapper::AtomicStoreIgnoreExist(counterFileName, counterMapContent);

        string subSegmentInfoPath = PathUtil::JoinPath(mergedDir,
                SUB_SEGMENT_DIR_NAME, SEGMENT_INFO_FILE_NAME);
        mergePlan.GetSubTargetSegmentInfo(targetSegIdx).StoreIgnoreExist(subSegmentInfoPath);

        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "EndMerge sub segment %d, segment info : %s", targetSegmentId,
                mergePlan.GetSubTargetSegmentInfo(targetSegIdx).ToString(true).c_str());
    }
}

FileList IndexPartitionMerger::ListInstanceDirs(const string &rootPath)
{
    FileList fileList;
    FileSystemWrapper::ListDir(rootPath, fileList);
    FileList instanceDirList;
    for (size_t i = 0; i < fileList.size(); i++)
    {
        string path = FileSystemWrapper::JoinPath(rootPath, fileList[i]);
        if (0 == fileList[i].find(MERGE_INSTANCE_DIR_PREFIX)
            && FileSystemWrapper::IsDir(path))
        {
            instanceDirList.push_back(path);
        }
    }
    return instanceDirList;
}

FileList IndexPartitionMerger::ListInstanceFiles(
        const string &instanceDir, const string &subDir)
{
    FileList fileList;
    string dirPath = FileSystemWrapper::JoinPath(instanceDir, subDir);
    try
    {
        FileSystemWrapper::ListDir(dirPath, fileList);
    }
    catch (misc::NonExistException& e)
    {
        return fileList;
    }
    catch (...)
    {
        throw;
    }

    for (size_t i = 0; i < fileList.size(); i++)
    {
        fileList[i] = FileSystemWrapper::JoinPath(dirPath, fileList[i]);
    }
    return fileList;
}

void IndexPartitionMerger::MoveFiles(const FileList &files, const string &targetDir)
{
    return DirectoryMerger::MoveFiles(files, targetDir);
}

void IndexPartitionMerger::SplitFileName(const string &input, string &folder, string &fileName)
{
    size_t found;
    found = input.find_last_of("/\\");
    if (found == string::npos) {
        fileName = input;
        return;
    }
    folder = input.substr(0,found);
    fileName = input.substr(found+1);
}

void IndexPartitionMerger::DeploySegmentIndex(
    const string& rootPath, const string& targetSegDirName, const string& lifecycle)
{
    DeployIndexWrapper deployIndexWrapper(rootPath);
    if (!deployIndexWrapper.DumpSegmentDeployIndex(targetSegDirName, lifecycle))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "dump segement deploy index failed, "
                             "rootPath [%s], targetSeg [%s]",
                             rootPath.c_str(), targetSegDirName.c_str());
    }
}

void IndexPartitionMerger::DeployPartitionInfo(const string& rootPath)
{
    DeployIndexWrapper deployIndexWrapper(rootPath);
    if (!deployIndexWrapper.DumpTruncateMetaIndex())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "dump truncate meta failed, rootPath [%s]", rootPath.c_str());
    }
    if (!deployIndexWrapper.DumpAdaptiveBitmapMetaIndex())
    {
        INDEXLIB_FATAL_ERROR(
            FileIO, "dump adaptive bitmap meta failed, rootPath [%s]", rootPath.c_str());
    }
}

void IndexPartitionMerger::ParallelEndMerge(
        const DirectoryPtr& rootDirectory, const IndexMergeMetaPtr &mergeMeta)
{
    ParallelEndMergeExecutor executor(mSchema, mPluginManager, rootDirectory);
    executor.ParallelEndMerge(mergeMeta);
}

void IndexPartitionMerger::SinglePartEndMerge(const string &rootPath,
        const vector<MergePlan> &mergePlans,
        const Version &targetVersion)
{
    assert(!rootPath.empty());
    for (size_t i = 0; i < mergePlans.size(); ++i)
    {
        const auto& plan = mergePlans[i];
        size_t targetSegmentCount = plan.GetTargetSegmentCount();
        for (size_t segIdx = 0; segIdx < targetSegmentCount; ++segIdx)
        {
            segmentid_t targetSegId = mergePlans[i].GetTargetSegmentId(segIdx);
            string targetSegDirName = targetVersion.GetSegmentDirName(targetSegId);
            const auto& metrics = mergePlans[i].GetTargetSegmentCustomizeMetrics(segIdx);
            auto it = metrics.find(LIFECYCLE);
            string lifecycle;
            if (it != metrics.end())
            {
                autil::legacy::FromJson(lifecycle, it->second);
            }
            DeploySegmentIndex(rootPath, targetSegDirName, lifecycle);
        }
    }
    DeployPartitionInfo(rootPath);

    Version commitVersion = targetVersion;
    commitVersion.SyncSchemaVersionId(mSchema);
    commitVersion.SetOngoingModifyOperations(mOptions.GetOngoingModifyOperationIds());
    VersionCommitter committer(rootPath, commitVersion, mKeepVersionCount,
                               mOptions.GetReservedVersions());
    committer.Commit();
}

SegmentMergeInfos IndexPartitionMerger::CreateSegmentMergeInfos(
        const SegmentDirectoryPtr &segDir) const
{
    return MergeMetaCreator::CreateSegmentMergeInfos(mSchema, mMergeConfig, segDir);
}

void IndexPartitionMerger::ReclaimDocuments(
    const SegmentDirectoryPtr &segDir, int64_t currentTs)
{
    DocumentReclaimer docReclaimer(segDir, mSchema, mOptions, currentTs, mMetricProvider);
    docReclaimer.Reclaim();
    const Version& version = segDir->GetVersion();
    mDumpStrategy->Reload(version.GetTimestamp(), version.GetLocator());
}

MergeTask IndexPartitionMerger::CreateMergeTaskByStrategy(
    bool optimize, const MergeConfig& mergeConfig,
    const SegmentMergeInfos& segMergeInfos, const LevelInfo& levelInfo) const
{
    MergeStrategyPtr mergeStrategy =
        MergeStrategyFactory::GetInstance()->CreateMergeStrategy(
                mergeConfig.mergeStrategyStr, mSegmentDirectory, mSchema);
    mergeStrategy->SetParameter(mergeConfig.mergeStrategyParameter);
    if (!optimize)
    {
        return mergeStrategy->CreateMergeTask(segMergeInfos, levelInfo);
    }
    else
    {
        return mergeStrategy->CreateMergeTaskForOptimize(segMergeInfos, levelInfo);
    }
}

MergeSchedulerPtr IndexPartitionMerger::CreateMergeScheduler(
        int64_t maxMemUse, uint32_t maxThreadNum)
{
    MergeSchedulerPtr scheduler;
    scheduler.reset(new MultiThreadedMergeScheduler(maxMemUse, maxThreadNum));
    return scheduler;
}

std::string IndexPartitionMerger::GetMergeIndexRoot() const
{
    return mDumpStrategy->GetRootDir();
}

std::string IndexPartitionMerger::GetMergeMetaDir() const
{
    return FileSystemWrapper::JoinPath(mDumpStrategy->GetRootDir(), "merge_meta");
}

void IndexPartitionMerger::MkDirIfNotExist(const string &dir)
{
    if (!FileSystemWrapper::IsExist(dir))
    {
        FileSystemWrapper::MkDir(dir, true);
    }
}

versionid_t IndexPartitionMerger::GetTargetVersionId(
        const Version& version, versionid_t alignVersionId)
{
    versionid_t lastVersionId = version.GetVersionId();
    if (alignVersionId == INVALID_VERSION)
    {
        //when full index no data, create empty version
        return lastVersionId == INVALID_VERSION ? 0 : lastVersionId;
    }

    if (lastVersionId > alignVersionId)
    {
        IE_LOG(ERROR, "align version id from [%d] to [%d] fail, alignVersion is smaller", lastVersionId, alignVersionId);
        return lastVersionId;
    }

    if (lastVersionId < alignVersionId)
    {
        IE_LOG(INFO, "align version id from [%d] to [%d]", lastVersionId, alignVersionId);
        return alignVersionId;
    }

    return lastVersionId;
}

void IndexPartitionMerger::AlignVersion(const string& rootDir,
                                        versionid_t alignVersionId)
{
    Version latestVersion;
    VersionLoader::GetVersion(rootDir, latestVersion, INVALID_VERSION);
    versionid_t targetVersionId = GetTargetVersionId(latestVersion, alignVersionId);
    if (latestVersion.GetVersionId() != targetVersionId)
    {
        if (latestVersion.GetVersionId() == INVALID_VERSION)
        {
            //init empty version level info
            LevelInfo& levelInfo = latestVersion.GetLevelInfo();
            const BuildConfig& buildConfig = mOptions.GetBuildConfig();
            levelInfo.Init(buildConfig.levelTopology, buildConfig.levelNum,
                           buildConfig.shardingColumnNum);
            latestVersion.SetTimestamp(mSegmentDirectory->GetVersion().GetTimestamp());
        }
        latestVersion.SetVersionId(targetVersionId);
        latestVersion.SyncSchemaVersionId(mSchema);
        latestVersion.SetOngoingModifyOperations(mOptions.GetOngoingModifyOperationIds());
        VersionCommitter committer(rootDir, latestVersion,
                mKeepVersionCount, mOptions.GetReservedVersions());
        committer.Commit();
    }
}

void IndexPartitionMerger::TEST_MERGE_WITH_CHECKPOINTS(
        bool optimize, const MergeMetaPtr &meta, uint32_t instanceId)
{
    // remove check points, will do merge when index dirs exist
    string instDir = GetInstanceTempMergeDir(mDumpStrategy->GetRootDir(), instanceId);
    string checkPointDir = FileSystemWrapper::JoinPath(
            instDir, MERGE_ITEM_CHECK_POINT_DIR_NAME);
    FileSystemWrapper::DeleteIfExist(checkPointDir);
    DoMerge(optimize, meta, instanceId);

    // with check points, will skip merge by check points
    DoMerge(optimize, meta, instanceId);
}

void IndexPartitionMerger::PrepareMerge(int64_t currentTs)
{

    if (!DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, mSegmentDirectory))
    {
        // TODO: add case
        // inc merge remove useless lost segments which segment id bigger than lastSegId in version
        // avoid useless segment effect build phrase (worker degrade scene)
        DirectoryPtr rootDir = mSegmentDirectory->GetPartitionData()->GetRootDirectory();
        OfflineRecoverStrategy::RemoveLostSegments(
            mSegmentDirectory->GetVersion(), rootDir);

        // remove instance dirs
        FileList instanceDirList = ListInstanceDirs(rootDir->GetPath());
        for (size_t i = 0; i < instanceDirList.size(); i++)
        {
            FileSystemWrapper::DeleteIfExist(instanceDirList[i]);
        }
    }

    IE_LOG(INFO, "begin reclaim documents");
    ReclaimDocuments(mSegmentDirectory, currentTs);
    IE_LOG(INFO, "reclaim documents done");
}

bool IndexPartitionMerger::IsTargetVersionValid(versionid_t versionId)
{
    Version version;
    try
    {
        VersionLoader::GetVersion(mDumpStrategy->GetRootDir(), version, versionId);
    }
    catch (...)
    {
        return false;
    }
    if (version.GetVersionId() == INVALID_VERSION)
    {
        return false;
    }
    return true;
}

IE_NAMESPACE_END(merger);
