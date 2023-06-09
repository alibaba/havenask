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
#include "indexlib/merger/index_partition_merger.h"

#include "beeper/beeper.h"
#include "indexlib/common/chunk/chunk_decoder_creator.h"
#include "indexlib/config/truncate_index_name_mapper.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/package/DirectoryMerger.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_factory.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index/segment_metrics_updater/multi_segment_metrics_updater.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/document_reclaimer/document_reclaimer.h"
#include "indexlib/merger/merge_meta_creator_factory.h"
#include "indexlib/merger/merge_meta_work_item.h"
#include "indexlib/merger/merge_strategy/merge_strategy_factory.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/merger/merge_task_item_dispatcher.h"
#include "indexlib/merger/merge_work_item_creator.h"
#include "indexlib/merger/merge_work_item_typed.h"
#include "indexlib/merger/merger_branch_hinter.h"
#include "indexlib/merger/multi_threaded_merge_scheduler.h"
#include "indexlib/merger/parallel_end_merge_executor.h"
#include "indexlib/merger/segment_metric_update_work_item.h"
#include "indexlib/merger/split_strategy/split_segment_strategy_factory.h"
#include "indexlib/plugin/plugin_factory_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/NumericUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/resource_control_thread_pool.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::merger;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::index;

using FSEC = indexlib::file_system::ErrorCode;

namespace indexlib { namespace merger {

IE_LOG_SETUP(merger, IndexPartitionMerger);

const string IndexPartitionMerger::MERGE_INSTANCE_DIR_PREFIX = "instance_";
const string IndexPartitionMerger::INSTANCE_TARGET_VERSION_FILE = "target_version";

IndexPartitionMerger::IndexPartitionMerger(const SegmentDirectoryPtr& segDir, const IndexPartitionSchemaPtr& schema,
                                           const IndexPartitionOptions& options, const DumpStrategyPtr& dumpStrategy,
                                           util::MetricProviderPtr metricProvider,
                                           const plugin::PluginManagerPtr& pluginManager,
                                           const CommonBranchHinterOption& branchOption, const PartitionRange& range)
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
    , mLastProgress(-1)
{
    assert(segDir);
    if (mDumpStrategy == NULL) {
        mDumpStrategy.reset(new DumpStrategy(segDir));
    }
    if (unlikely(!mSegmentDirectory->GetRootDir()->GetFileSystem()->GetFileSystemOptions().isOffline)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "merger should not use online filesystem path [%s]",
                             mSegmentDirectory->GetRootDir()->DebugString().c_str());
    }
    // TODO: @qingran check
    assert(DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, mSegmentDirectory) ||
           mDumpStrategy->GetRootPath() == mSegmentDirectory->GetRootDir()->GetOutputPath());
    file_system::FslibWrapper::SetMergeIOConfig(mMergeConfig.mergeIOConfig);
    ChunkDecoderCreator::Reset();

    mSubPartSegmentDirectory = mSegmentDirectory->GetSubSegmentDirectory();
    if (mPluginManager) {
        plugin::PluginResourcePtr pluginResource = mPluginManager->GetPluginResource();
        assert(pluginResource);
        pluginResource->counterMap = mCounterMap;
    }
    mFsOptions = FileSystemOptions::OfflineWithBlockCache(mMetricProvider);
    mFsOptions.enableAsyncFlush = mMergeConfig.mergeIOConfig.enableAsyncWrite;
    mFsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();

    if (mMergeConfig.GetEnablePackageFile()) {
        mFsOptions.outputStorage = FSST_PACKAGE_DISK;
        mFsOptions.packageFileTagConfigList =
            std::make_shared<PackageFileTagConfigList>(mMergeConfig.GetPackageFileTagConfigList());
    } else {
        mFsOptions.outputStorage = FSST_DISK;
    }
    mDeletionReader.reset(new DeletionMapReader());
    mHinter.reset(new MergerBranchHinter(branchOption));
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
    if (envStr && std::string(envStr) == "true") {
        mDumpStrategy->RevertVersion(tmpVersion);
        PrepareMerge(currentTs);
        MergeMetaPtr meta = CreateMergeMeta(optimize, 1, currentTs);
        TEST_MERGE_WITH_CHECKPOINTS(optimize, meta, 0);
    }
    EndMerge(meta);
    return meta->GetMergePlanCount() != 0;
}

void IndexPartitionMerger::CleanTargetDirs(const MergeMetaPtr& mergeMeta)
{
    const Version& targetVersion = mergeMeta->GetTargetVersion();
    size_t planCount = mergeMeta->GetMergePlanCount();
    for (size_t i = 0; i < planCount; i++) {
        auto targetSegmentIds = mergeMeta->GetTargetSegmentIds(i);
        for (const segmentid_t segId : targetSegmentIds) {
            auto targetSegment = targetVersion.GetSegmentDirName(segId);
            file_system::DirectoryPtr partitionDir = mDumpStrategy->GetRootDirectory();
            indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
            partitionDir->RemoveDirectory(targetSegment, removeOption /*mayNonExsit*/);
        }
        // no need to delete version file, commit version will remove old first
    }
}

IndexMergeMeta* IndexPartitionMerger::CreateMergeMetaWithTask(MergeTask& task, const SegmentMergeInfos& segMergeInfos,
                                                              bool optimize, uint32_t instanceCount)
{
    const auto& updaterConfig = mOptions.GetBuildConfig().GetSegmentMetricsUpdaterConfig();
    SplitSegmentStrategyFactory* splitStrategyFactory = nullptr;
    const auto& splitConfig = mMergeConfig.GetSplitSegmentConfig();
    SplitSegmentStrategyFactory factory;
    if (plugin::PluginManager::isBuildInModule(splitConfig.moduleName)) {
        splitStrategyFactory = &factory;
    } else {
        splitStrategyFactory =
            plugin::PluginFactoryLoader::GetFactory<SplitSegmentStrategyFactory, SplitSegmentStrategyFactory>(
                splitConfig.moduleName, SplitSegmentStrategyFactory::SPLIT_SEGMENT_STRATEGY_FACTORY_SUFFIX,
                mPluginManager);
    }
    if (!splitStrategyFactory) {
        INDEXLIB_FATAL_ERROR(Runtime, "get split segment strategy factory from moudle [%s] failed",
                             splitConfig.moduleName.c_str());
    }
    mAttrReaderContainer->Reset();
    auto generateUpdater = [segMergeInfos, updaterConfig, this]() mutable {
        MultiSegmentMetricsUpdaterPtr updater(new MultiSegmentMetricsUpdater(mMetricProvider));
        updater->InitForMerge(mSchema, mOptions, segMergeInfos, mAttrReaderContainer, updaterConfig, mPluginManager);
        return updater;
    };
    splitStrategyFactory->Init(mSegmentDirectory, mSchema, mAttrReaderContainer, segMergeInfos,
                               std::move(generateUpdater), mUpdaters, mMetricProvider);

    MergeMetaCreatorPtr mergeMetaCreator =
        MergeMetaCreatorFactory::Create(mSchema, CreateReclaimMapCreator(), splitStrategyFactory);
    mergeMetaCreator->Init(mSegmentDirectory, segMergeInfos, mMergeConfig, mDumpStrategy, mPluginManager,
                           instanceCount);
    return mergeMetaCreator->Create(task);
}

MergeMetaPtr IndexPartitionMerger::CreateMergeMeta(bool optimize, uint32_t instanceCount, int64_t currentTs)
{
    string phase = "begin_merge";
    string beginMergeCheckpointRoot = GetCheckpointDirForPhase(phase);

    mAttrReaderContainer.reset(new OfflineAttributeSegmentReaderContainer(mSchema, mSegmentDirectory));
    mSubPartSegmentDirectory = mSegmentDirectory->GetSubSegmentDirectory();
    SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos(mSegmentDirectory);
    IE_LOG(INFO, "create segmentMergeInfos done");
    if (segMergeInfos.size() == 0) {
        IE_LOG(INFO, "nothing to merge");
        return MergeMetaPtr(new IndexMergeMeta());
    }
    if (mMergeConfig.NeedCalculateTemperature()) {
        ReCalculatorTemperature(segMergeInfos, beginMergeCheckpointRoot);
    }

    indexlibv2::framework::LevelInfo levelInfo = mSegmentDirectory->GetVersion().GetLevelInfo();
    MergeTask task = CreateMergeTaskByStrategy(optimize, mMergeConfig, segMergeInfos, levelInfo);
    IE_LOG(INFO, "create MergeTask done");
    IndexMergeMeta* meta = CreateMergeMetaWithTask(task, segMergeInfos, optimize, instanceCount);
    IE_LOG(INFO, "create MergeMetaWithTask done");
    meta->SetTimestamp(currentTs);
    try {
        meta->SetCounterMap(mSegmentDirectory->GetLatestCounterMapContent());
    } catch (...) {
        DELETE_AND_SET_NULL(meta);
        throw;
    }
    IE_LOG(INFO, "end create mergeMeta");
    return MergeMetaPtr(meta);
}

void IndexPartitionMerger::ReCalculatorTemperature(SegmentMergeInfos& segMergeInfos, const string& checkpointDir)
{
    string recalculateCheckpointDir;
    if (!checkpointDir.empty()) {
        recalculateCheckpointDir = util::PathUtil::JoinPath(checkpointDir, "recalculate_temperature");
        auto ret = file_system::FslibWrapper::MkDirIfNotExist(recalculateCheckpointDir).Code();
        if (ret != file_system::FSEC_OK && ret != file_system::FSEC_EXIST) {
            INDEXLIB_FATAL_ERROR(FileIO, "makeDir if not exist for [%s] error.", recalculateCheckpointDir.c_str());
        }
    }

    mMetrics.BeginRecaculatorRunningTime();
    IE_LOG(INFO, "recalculator temperature begin");
    mDeletionReader->Open(mSegmentDirectory->GetPartitionData().get());
    const auto& updaterConfig = mOptions.GetBuildConfig().GetSegmentMetricsUpdaterConfig();
    mUpdaters.clear();
    ResourceControlThreadPool threadPool(min((uint32_t)segMergeInfos.size(), mMergeConfig.mergeThreadCount), 0);
    vector<ResourceControlWorkItemPtr> workItems;
    FenceContextPtr fenceContext = mDumpStrategy->GetRootDirectory()->GetFenceContext();

    for (auto& segMergInfo : segMergeInfos) {
        MultiSegmentMetricsUpdaterPtr updater(new MultiSegmentMetricsUpdater(mMetricProvider));
        updater->InitForReCalculator(mSchema, mAttrReaderContainer, mDeletionReader, updaterConfig, segMergInfo,
                                     segMergInfo.segmentId, mPluginManager);
        mUpdaters.insert(std::make_pair(segMergInfo.segmentId, updater));
        ResourceControlWorkItemPtr item(
            new SegmentMetricUpdateWorkItem(updater, recalculateCheckpointDir, fenceContext.get()));
        workItems.push_back(item);
    }
    threadPool.Init(workItems);
    IE_LOG(INFO, "pool inited");
    threadPool.Run("segmentMetricUpdate");
    IE_LOG(INFO, "pool finished");
    workItems.clear();

    for (auto& segMergInfo : segMergeInfos) {
        if (!mUpdaters[segMergInfo.segmentId]->UpdateSegmentMetric(segMergInfo.segmentMetrics)) {
            INDEXLIB_FATAL_ERROR(Runtime, "segment [%d] update segment metric failed, old segment metric not exist",
                                 segMergInfo.segmentId);
        }
        SegmentTemperatureMeta meta;
        if (SegmentTemperatureMeta::InitFromSegmentMetric(segMergInfo.segmentMetrics, meta)) {
            meta.segmentId = segMergInfo.segmentId;
            mDumpStrategy->AddSegmentTemperatureMeta(meta);
        } else {
            INDEXLIB_FATAL_ERROR(Runtime, "segment [%d] segment metric is invalid, segmentMetrics [%s]",
                                 segMergInfo.segmentId, ToJsonString(segMergInfo.segmentMetrics).c_str());
        }
    }
    mMetrics.EndRecaculatorRunningTime();
    IE_LOG(INFO, "recalculator temperature end");
}

MergeMetaPtr IndexPartitionMerger::LoadMergeMeta(const string& mergeMetaPath, bool onlyLoadBasicInfo)
{
    MergeMetaPtr mergeMeta(new IndexMergeMeta());
    if (onlyLoadBasicInfo) {
        return mergeMeta->LoadBasicInfo(mergeMetaPath) ? mergeMeta : MergeMetaPtr();
    }
    return mergeMeta->Load(mergeMetaPath) ? mergeMeta : MergeMetaPtr();
}

void IndexPartitionMerger::DoMerge(bool optimize, const MergeMetaPtr& mergeMeta, uint32_t instanceId)
{
    IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);
    if (instanceId >= indexMergeMeta->GetMergeTaskItemsSize()) {
        IE_LOG(INFO, "nothing to merge for instance[%d].", instanceId);
        return;
    }

    versionid_t fromVersionId = mSegmentDirectory->GetVersion().GetVersionId();
    versionid_t targetVersionId = indexMergeMeta->GetTargetVersion().GetVersionId();
    vector<segmentid_t> mergeFrom;
    vector<segmentid_t> mergeTo;
    auto mergePlans = indexMergeMeta->GetMergePlans();
    for (auto& mergePlan : mergePlans) {
        auto iter = mergePlan.CreateIterator();
        while (iter.HasNext()) {
            mergeFrom.push_back(iter.Next());
        }
        for (size_t i = 0; i < mergePlan.GetTargetSegmentCount(); i++) {
            mergeTo.push_back(mergePlan.GetTargetSegmentId(i));
        }
    }
    mMetrics.SetMergeVersionInfo(mSchema->GetSchemaName(), fromVersionId, targetVersionId, mergeFrom, mergeTo);

    MergeFileSystemPtr mergeFileSystem = CreateMergeFileSystem(instanceId, mergeMeta);
    IE_LOG(INFO, "prepare MergeDir done");
    const MergeTaskItems& mergeTaskItems = indexMergeMeta->GetMergeTaskItems(instanceId);
    MergeWorkItemCreatorPtr creator = CreateMergeWorkItemCreator(optimize, *indexMergeMeta, mergeFileSystem);

    vector<ResourceControlWorkItemPtr> mergeWorkItems;
    uint32_t itemCount = 0;
    bool needBlockCache = false;
    for (MergeTaskItems::const_iterator it = mergeTaskItems.begin(); it != mergeTaskItems.end(); ++it) {
        MergeWorkItemPtr mergeItem(creator->CreateMergeWorkItem(*it));
        if (mergeItem.get() == NULL) {
            continue;
        }
        if (it->mMergeType == ATTRIBUTE_TASK_NAME || it->mMergeType == PACK_ATTRIBUTE_TASK_NAME) {
            needBlockCache = true;
        }
        mergeWorkItems.push_back(mergeItem);
        ++itemCount;
    }

    if (itemCount == 0) {
        IE_LOG(WARN,
               "no merge task item for current instance [%u],"
               " maybe start too many merge instance!",
               instanceId);
    }
    mMetrics.StartReport(mCounterMap);
    IE_LOG(INFO, "create MergeWorkItem done");
    int64_t maxMemUse = mMergeConfig.maxMemUseForMerge * 1024 * 1024;
    maxMemUse -= mergeMeta->EstimateMemoryUse(); // reclaim map and bucket map
    if (needBlockCache) {
        maxMemUse -= mFsOptions.fileBlockCacheContainer->GetResourceInfo().maxMemoryUse;
    }
    CreateBranchProgressLoop();
    MergeSchedulerPtr scheduler = CreateMergeScheduler(maxMemUse, mMergeConfig.mergeThreadCount);
    scheduler->Run(mergeWorkItems, mergeFileSystem);
    mergeFileSystem->Close();
    if (mSyncBranchProgressThread) {
        mSyncBranchProgressThread->stop();
    }
    CommitToDefaultBranch(instanceId);
    mMetrics.StopReport();
}

void IndexPartitionMerger::CreateBranchProgressLoop()
{
    if (!mMergeConfig.enableMergeItemCheckPoint) {
        return;
    }
    mSyncBranchProgressThread = autil::LoopThread::createLoopThread(
        bind(&IndexPartitionMerger::SyncBranchProgress, this), 30 * 1000 * 1000 /*30s */, "IndexSyncCheckpoint");
}

void IndexPartitionMerger::SyncBranchProgress()
{
    auto count = mCounterMap->GetStateCounter("offline.mergeProgress");
    int64_t currentProgress = count->Get();
    if (currentProgress != mLastProgress) {
        DYNAMIC_POINTER_CAST(MergerBranchHinter, mHinter)
            ->StorePorgressIfNeed(mBranchFs->GetRootPath(), mBranchFs->GetBranchName(), currentProgress);
        mLastProgress = currentProgress;
        IE_LOG(INFO, "sync progress [%ld]", currentProgress);
    }
}

void IndexPartitionMerger::PrepareMergeDir(uint32_t instanceId, const MergeMetaPtr& mergeMeta, string& instanceRootPath)
{
    file_system::DirectoryPtr rootDir = mDumpStrategy->GetRootDirectory();
    file_system::ErrorCode ec = file_system::ErrorCode::FSEC_OK;
    instanceRootPath = GetInstanceMergeDir(mDumpStrategy->GetRootPath(), instanceId);

    string instTargetVersion = util::PathUtil::JoinPath(instanceRootPath, INSTANCE_TARGET_VERSION_FILE);
    const Version& targetVersion = mergeMeta->GetTargetVersion();
    Version instVersion;

    // any two workers in same instance, the one with higher target version has authority
    // old target version worker (maybe dead worker) will not remove other workers' path
    if (!instVersion.Load(instTargetVersion) || instVersion < targetVersion) {
        IE_LOG(INFO, "target_version in instance path [%s] not exist!", instanceRootPath.c_str());

        FenceContext* instanceContext =
            rootDir->GetFenceContext()
                ? FslibWrapper::CreateFenceContext(instanceRootPath, rootDir->GetFenceContext()->epochId)
                : FenceContext::NoFence();

        unique_ptr<FenceContext> fenceContextGuard(instanceContext);
        ec = FslibWrapper::DeleteDir(instanceRootPath, DeleteOption::Fence(instanceContext, true)).Code();
        THROW_IF_FS_ERROR(ec, "delete dir failed, path[%s]", instanceRootPath.c_str());

        ec = FslibWrapper::MkDir(instanceRootPath).Code();
        THROW_IF_FS_ERROR(ec, "mk dir failed, path[%s]", instanceRootPath.c_str());

        if (instanceContext) {
            if (!FslibWrapper::UpdateFenceInlineFile(instanceContext)) {
                INDEXLIB_FATAL_ERROR(FileIO, "update instance path [%s] fence context epochId [%s] failed",
                                     instanceRootPath.c_str(), instanceContext->epochId.c_str());
            }
        }

        bool removeIfExist = true;
        ec = FslibWrapper::AtomicStore(instTargetVersion, targetVersion.ToString(), removeIfExist, instanceContext)
                 .Code();
        THROW_IF_FS_ERROR(ec, "atomic store failed, path[%s]", instTargetVersion.c_str());
    }
}

string IndexPartitionMerger::GetInstanceMergeDir(const string& rootDir, uint32_t instanceId)
{
    string curInstanceDir = MERGE_INSTANCE_DIR_PREFIX + StringUtil::toString(instanceId);
    return util::PathUtil::JoinPath(rootDir, curInstanceDir);
}

MergeWorkItemCreatorPtr IndexPartitionMerger::CreateMergeWorkItemCreator(bool optimize, const IndexMergeMeta& mergeMeta,
                                                                         const MergeFileSystemPtr& mergeFileSystem)
{
    return MergeWorkItemCreatorPtr(new MergeWorkItemCreator(
        mSchema, mMergeConfig, &mergeMeta, mSegmentDirectory, mSubPartSegmentDirectory, IsSortMerge(), optimize,
        &mMetrics, mCounterMap, mOptions, mergeFileSystem, mPluginManager));
}

MergeFileSystemPtr IndexPartitionMerger::CreateMergeFileSystem(uint32_t instanceId, const MergeMetaPtr& mergeMeta)
{
    string instanceRootPath;
    PrepareMergeDir(instanceId, mergeMeta, instanceRootPath);

    mBranchFs = BranchFS::CreateWithAutoFork(instanceRootPath, mFsOptions, mMetricProvider, mHinter.get());
    mFileSystem = mBranchFs->GetFileSystem();

    bool hasSub = mSchema->GetSubIndexPartitionSchema().operator bool();
    vector<string> targetSegmentPaths;
    const index_base::Version& targetVersion = mergeMeta->GetTargetVersion();
    for (size_t planIdx = 0; planIdx < mergeMeta->GetMergePlanCount(); ++planIdx) {
        const std::vector<segmentid_t>& targetSegmentIds = mergeMeta->GetTargetSegmentIds(planIdx);
        for (segmentid_t segmentId : targetSegmentIds) {
            string targetSegName = targetVersion.GetSegmentDirName(segmentId);
            targetSegmentPaths.push_back(targetSegName);
            if (hasSub) {
                targetSegmentPaths.push_back(PathUtil::JoinPath(targetSegName, SUB_SEGMENT_DIR_NAME));
            }
        }
    }

    MergeFileSystemPtr mergeFileSystem =
        MergeFileSystem::Create(instanceRootPath, mMergeConfig, instanceId, mFileSystem);
    mergeFileSystem->Init(targetSegmentPaths);
    return mergeFileSystem;
}

void IndexPartitionMerger::EndMerge(const MergeMetaPtr& mergeMeta, versionid_t alignVersionId)
{
    IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, mergeMeta);
    const vector<MergePlan>& mergePlans = indexMergeMeta->GetMergePlans();
    const Version& targetVersion = indexMergeMeta->GetTargetVersion();

    if (targetVersion == index_base::Version(INVALID_VERSION)) {
        const auto rootDir = mDumpStrategy->GetRootDirectory();
        AlignVersion(rootDir, alignVersionId);
        rootDir->Close();
        return;
    }
    assert(targetVersion.GetVersionId() != INVALID_VERSION);
    Version alignVersion = targetVersion;
    alignVersion.SetVersionId(GetTargetVersionId(targetVersion, alignVersionId));
    alignVersion.SyncSchemaVersionId(mSchema);

    if (IsTargetVersionValid(alignVersion.GetVersionId())) {
        // target version is generated, not need to do end merge again
        return;
    }

    const file_system::DirectoryPtr& rootDirectory = mDumpStrategy->GetRootDirectory();
    assert(rootDirectory != nullptr);
    // merge instance merge dir to temp merge dir
    bool hasSub = (mSubPartSegmentDirectory != NULL);
    MergeInstanceDir(rootDirectory, mergePlans, targetVersion, indexMergeMeta->GetCounterMapContent(), hasSub);
    string adaptiveDictDirPath = PathUtil::JoinPath(mDumpStrategy->GetRootPath(), ADAPTIVE_DICT_DIR_NAME);
    string truncateMetaDirPath = PathUtil::JoinPath(mDumpStrategy->GetRootPath(), TRUNCATE_META_DIR_NAME);
    if (FslibWrapper::IsExist(adaptiveDictDirPath).GetOrThrow()) {
        THROW_IF_FS_ERROR(rootDirectory->GetFileSystem()->MountDir(mDumpStrategy->GetRootPath(), ADAPTIVE_DICT_DIR_NAME,
                                                                   ADAPTIVE_DICT_DIR_NAME, FSMT_READ_WRITE, true),
                          "mount dir[%s] failed", ADAPTIVE_DICT_DIR_NAME);
    }
    if (FslibWrapper::IsExist(truncateMetaDirPath).GetOrThrow()) {
        THROW_IF_FS_ERROR(rootDirectory->GetFileSystem()->MountDir(mDumpStrategy->GetRootPath(), TRUNCATE_META_DIR_NAME,
                                                                   TRUNCATE_META_DIR_NAME, FSMT_READ_WRITE, true),
                          "mount dir[%s] failed", TRUNCATE_META_DIR_NAME);
        DirectoryPtr truncateMetaDir = mDumpStrategy->GetRootDirectory()->GetDirectory(TRUNCATE_META_DIR_NAME, true);
        TruncateIndexNameMapper truncateIndexMapper(truncateMetaDir);
        truncateIndexMapper.Dump(mSchema->GetIndexSchema());
        truncateMetaDir->Close();
    }
    ParallelEndMerge(rootDirectory, indexMergeMeta);
    SinglePartEndMerge(rootDirectory, mergePlans, alignVersion);
    rootDirectory->Close();
}

framework::SegmentMetrics IndexPartitionMerger::CollectSegmentMetrics(const DirectoryPtr& targetSegmentDir,
                                                                      const MergePlan& plan, size_t targetSegIdx)
{
    // TODO: list all field dir, merge these metrics into one
    indexlib::framework::SegmentMetrics segMetrics;
    if (mSchema->GetTableType() != tt_kv && mSchema->GetTableType() != tt_kkv) {
        segMetrics.SetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP,
                                          plan.GetTargetSegmentCustomizeMetrics(targetSegIdx));
        return segMetrics;
    }
    DirectoryPtr indexDir = targetSegmentDir->GetDirectory(INDEX_DIR_NAME, true);
    segMetrics.Load(indexDir);
    return segMetrics;
}

void IndexPartitionMerger::MergeInstanceDir(const DirectoryPtr& rootDirectory, const vector<MergePlan>& mergePlans,
                                            const Version& targetVersion, const string& counterMapContent, bool hasSub)
{
    const string& rootPath = mDumpStrategy->GetRootPath();
    FileList instanceDirNames = ListInstanceDirNames(rootPath);
    DirectoryVector instanceBranchDirectorys;

    for (const std::string& instanceDirName : instanceDirNames) {
        string branchName;
        instanceBranchDirectorys.emplace_back(BranchFS::GetDirectoryFromDefaultBranch(
            PathUtil::JoinPath(rootPath, instanceDirName), mFsOptions, mHinter.get(), &branchName));
        if (!mHinter->CanOperateBranch(branchName)) {
            INDEXLIB_FATAL_ERROR(Runtime, "old worker with epochId [%s] cannot move new branch [%s] with epochId [%s]",
                                 mHinter->GetOption().epochId.c_str(), branchName.c_str(),
                                 CommonBranchHinter::ExtractEpochFromBranch(branchName).c_str());
        }
    }

    for (const auto& instanceDir : instanceBranchDirectorys) {
        if (instanceDir->IsExist(TRUNCATE_META_DIR_NAME)) {
            FileList metaFiles;
            instanceDir->ListDir(TRUNCATE_META_DIR_NAME, metaFiles, false);
            if (!metaFiles.empty()) {
                string truncateMetaPath = instanceDir->GetPhysicalPath(TRUNCATE_META_DIR_NAME);
                THROW_IF_FS_ERROR(rootDirectory->GetFileSystem()->MergeDirs({truncateMetaPath}, TRUNCATE_META_DIR_NAME,
                                                                            false,
                                                                            rootDirectory->GetFenceContext().get()),
                                  "merge truncate meta dirs failed");
            }
        }
        if (instanceDir->IsExist(ADAPTIVE_DICT_DIR_NAME)) {
            FileList metaFiles;
            instanceDir->ListDir(ADAPTIVE_DICT_DIR_NAME, metaFiles, false);
            if (!metaFiles.empty()) {
                string adaptiveBitmapPath = instanceDir->GetPhysicalPath(ADAPTIVE_DICT_DIR_NAME);
                THROW_IF_FS_ERROR(rootDirectory->GetFileSystem()->MergeDirs({adaptiveBitmapPath},
                                                                            ADAPTIVE_DICT_DIR_NAME, false,
                                                                            rootDirectory->GetFenceContext().get()),
                                  "merge adaptive dict dir failed");
            }
        }
    }

    for (const MergePlan& plan : mergePlans) {
        size_t targetSegmentCount = plan.GetTargetSegmentCount();
        for (size_t segIdx = 0; segIdx < targetSegmentCount; ++segIdx) {
            segmentid_t targetSegmentId = plan.GetTargetSegmentId(segIdx);
            string segDirName = targetVersion.GetSegmentDirName(targetSegmentId);
            for (const auto& instanceDir : instanceBranchDirectorys) {
                auto segmentDir = instanceDir->GetDirectory(segDirName, true);
                // moved file must be checkpoint level
                if (hasSub) {
                    auto subDir = segmentDir->GetDirectory(SUB_SEGMENT_DIR_NAME, false);
                    if (subDir) {
                        MergeSegmentDir(rootDirectory, subDir, PathUtil::JoinPath(segDirName, SUB_SEGMENT_DIR_NAME));
                    }
                }
                MergeSegmentDir(rootDirectory, segmentDir, segDirName);
            }
            THROW_IF_FS_ERROR(
                rootDirectory->GetFileSystem()->MergePackageFiles(segDirName, rootDirectory->GetFenceContext().get()),
                "merge package files failed");
            if (hasSub) {
                THROW_IF_FS_ERROR(
                    rootDirectory->GetFileSystem()->MergePackageFiles(
                        PathUtil::JoinPath(segDirName, SUB_SEGMENT_DIR_NAME), rootDirectory->GetFenceContext().get()),
                    "merge package files failed");
            }
            DirectoryPtr targetSegmentDir = rootDirectory->GetDirectory(segDirName, true);
            auto segMetrics = CollectSegmentMetrics(targetSegmentDir, plan, segIdx);
            StoreSegmentInfos(plan, segIdx, targetSegmentDir, hasSub, counterMapContent, segMetrics);
            targetSegmentDir->Close();
        }
    }

    // todo
    for (const string& instanceName : instanceDirNames) {
        string instanceRoot = PathUtil::JoinPath(mDumpStrategy->GetRootPath(), instanceName);
        auto ec =
            FslibWrapper::DeleteDir(instanceRoot, DeleteOption::Fence(rootDirectory->GetFenceContext().get(), true))
                .Code();
        if (unlikely(FSEC_OK != ec)) {
            THROW_IF_FS_ERROR(ec, "remove [%s] failed", instanceRoot.c_str());
        }
    }
}

void IndexPartitionMerger::MergeSegmentDir(const file_system::DirectoryPtr& rootDirectory,
                                           const file_system::DirectoryPtr& segmentDirectory, const string& segmentPath)
{
    FenceContextPtr fenceContext = rootDirectory->GetFenceContext();
    IE_LOG(INFO, "begin merge segmentDir[%s] from [%s]", segmentPath.c_str(),
           segmentDirectory->GetLogicalPath().c_str());
    FileList fileList;
    segmentDirectory->ListDir("", fileList, false);
    for (const auto& fileName : fileList) {
        if (fileName == SUB_SEGMENT_DIR_NAME) {
            continue;
        } else {
            if (segmentDirectory->IsDir(fileName)) {
                if (mMergeConfig.GetEnablePackageFile()) {
                    // skip dir within package
                    continue;
                }
                FileList subFileList;
                segmentDirectory->ListDir(fileName, subFileList, false);
                if (!rootDirectory->IsExist(PathUtil::JoinPath(segmentPath, fileName))) {
                    rootDirectory->MakeDirectory(PathUtil::JoinPath(segmentPath, fileName))->Close();
                }
                for (const auto& subFileName : subFileList) {
                    string physicalPath = segmentDirectory->GetPhysicalPath(PathUtil::JoinPath(fileName, subFileName));
                    THROW_IF_FS_ERROR(rootDirectory->GetFileSystem()->MergeDirs(
                                          {physicalPath}, PathUtil::JoinPath(segmentPath, fileName, subFileName), false,
                                          fenceContext.get()),
                                      "merge dirs failed");
                }
            } else {
                string physicalPath = segmentDirectory->GetPhysicalPath(fileName);
                THROW_IF_FS_ERROR(rootDirectory->GetFileSystem()->MergeDirs({physicalPath},
                                                                            PathUtil::JoinPath(segmentPath, fileName),
                                                                            false, fenceContext.get()),
                                  "merge dirs failed");
            }
        }
    }
}

ReclaimMapCreatorPtr IndexPartitionMerger::CreateReclaimMapCreator()
{
    return ReclaimMapCreatorPtr(new ReclaimMapCreator(mMergeConfig.truncateOptionConfig != NULL));
}

void IndexPartitionMerger::StoreSegmentInfos(const MergePlan& mergePlan, size_t targetSegIdx,
                                             const file_system::DirectoryPtr& mergerDirectory, bool hasSub,
                                             const string& counterMapContent,
                                             const indexlib::framework::SegmentMetrics& segmentMetrics)
{
    segmentMetrics.Store(mergerDirectory);
    uint64_t keyCount;
    const SegmentTopologyInfo& topoLogoInfo = mergePlan.GetTargetTopologyInfo(targetSegIdx);
    string groupName = SegmentWriter::GetMetricsGroupName(topoLogoInfo.columnIdx);

    string keyMetricsName = "key_count";
    if (mSchema->GetTableType() == tt_kkv) {
        keyMetricsName = index::KKV_SKEY_COUNT;
    }

    WriterOption counterWriterOption;
    counterWriterOption.notInPackage = true;
    segmentid_t targetSegmentId = mergePlan.GetTargetSegmentId(targetSegIdx);
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    mergerDirectory->RemoveFile(COUNTER_FILE_NAME, removeOption);
    mergerDirectory->Store(COUNTER_FILE_NAME, counterMapContent, counterWriterOption);

    if (segmentMetrics.Get(groupName, keyMetricsName, keyCount)) {
        SegmentInfo segmentInfo = mergePlan.GetTargetSegmentInfo(targetSegIdx);
        segmentInfo.docCount = keyCount;
        segmentInfo.schemaId = mSchema->GetSchemaVersionId();
        segmentInfo.Store(mergerDirectory, true);
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "EndMerge segment %d, segment info : %s",
                                          targetSegmentId, segmentInfo.ToString(true).c_str());
        return;
    }

    mergePlan.GetTargetSegmentInfo(targetSegIdx).Store(mergerDirectory, true);
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "EndMerge segment %d, segment info : %s",
                                      targetSegmentId,
                                      mergePlan.GetTargetSegmentInfo(targetSegIdx).ToString(true).c_str());
    if (hasSub) {
        auto subDirectory = mergerDirectory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        if (subDirectory == nullptr) {
            IE_LOG(ERROR, "make directory failed, path[%s]", SUB_SEGMENT_DIR_NAME);
            return;
        }
        subDirectory->RemoveFile(COUNTER_FILE_NAME, removeOption);
        subDirectory->Store(COUNTER_FILE_NAME, counterMapContent, counterWriterOption);
        auto subSegInfoDir = mergerDirectory->GetDirectory(SUB_SEGMENT_DIR_NAME, false);
        if (subSegInfoDir == nullptr) {
            INDEXLIB_FATAL_ERROR(Runtime, "get directory failed, empty dir[%s/%s]",
                                 mergerDirectory->DebugString().c_str(), SUB_SEGMENT_DIR_NAME);
        }
        mergePlan.GetSubTargetSegmentInfo(targetSegIdx).Store(subSegInfoDir, true);
        subDirectory->Close();

        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                          "EndMerge sub segment %d, segment info : %s", targetSegmentId,
                                          mergePlan.GetSubTargetSegmentInfo(targetSegIdx).ToString(true).c_str());
    }
}

FileList IndexPartitionMerger::ListInstanceDirNames(const std::string& rootPath)
{
    FileList fileList;
    auto ec = FslibWrapper::ListDir(rootPath, fileList).Code();
    THROW_IF_FS_ERROR(ec, "list root dir failed, [%s]", rootPath.c_str());
    FileList instanceDirList;
    for (size_t i = 0; i < fileList.size(); i++) {
        string path = PathUtil::JoinPath(rootPath, fileList[i]);
        if (0 == fileList[i].find(MERGE_INSTANCE_DIR_PREFIX)) {
            if (FslibWrapper::IsDir(path).GetOrThrow()) {
                instanceDirList.push_back(fileList[i]);
            }
        }
    }
    return instanceDirList;
}

void IndexPartitionMerger::SplitFileName(const string& input, string& folder, string& fileName)
{
    size_t found;
    found = input.find_last_of("/\\");
    if (found == string::npos) {
        fileName = input;
        return;
    }
    folder = input.substr(0, found);
    fileName = input.substr(found + 1);
}

void IndexPartitionMerger::DeployPartitionInfo(const file_system::DirectoryPtr& rootDir)
{
    if (!DeployIndexWrapper::DumpTruncateMetaIndex(rootDir)) {
        INDEXLIB_FATAL_ERROR(FileIO, "dump truncate meta failed, rootPath [%s]", rootDir->DebugString().c_str());
    }
    if (!DeployIndexWrapper::DumpAdaptiveBitmapMetaIndex(rootDir)) {
        INDEXLIB_FATAL_ERROR(FileIO, "dump adaptive bitmap meta failed, rootPath [%s]", rootDir->DebugString().c_str());
    }
}

void IndexPartitionMerger::ParallelEndMerge(const DirectoryPtr& rootDirectory, const IndexMergeMetaPtr& mergeMeta)
{
    ParallelEndMergeExecutor executor(mSchema, mPluginManager, rootDirectory);
    executor.ParallelEndMerge(mergeMeta);
}

void IndexPartitionMerger::SinglePartEndMerge(const file_system::DirectoryPtr& rootDirectory,
                                              const vector<MergePlan>& mergePlans, const Version& targetVersion)
{
    assert(rootDirectory != nullptr);
    for (size_t i = 0; i < mergePlans.size(); ++i) {
        const auto& plan = mergePlans[i];
        size_t targetSegmentCount = plan.GetTargetSegmentCount();
        for (size_t segIdx = 0; segIdx < targetSegmentCount; ++segIdx) {
            segmentid_t targetSegId = mergePlans[i].GetTargetSegmentId(segIdx);
            string targetSegDirName = targetVersion.GetSegmentDirName(targetSegId);
            const auto& metrics = mergePlans[i].GetTargetSegmentCustomizeMetrics(segIdx);
            auto it = metrics.find(LIFECYCLE);
            string lifecycle;
            if (it != metrics.end()) {
                autil::legacy::FromJson(lifecycle, it->second);
            }
            file_system::DirectoryPtr targetSegDir = rootDirectory->GetDirectory(targetSegDirName, false);
            if (targetSegDir == nullptr) {
                INDEXLIB_FATAL_ERROR(FileIO,
                                     "dump segement deploy index failed, "
                                     "rootPath [%s], targetSeg [%s]",
                                     rootDirectory->DebugString().c_str(), targetSegDirName.c_str());
            }
            DeployIndexWrapper::DumpSegmentDeployIndex(targetSegDir, lifecycle);
            targetSegDir->Close();
        }
    }
    DeployPartitionInfo(rootDirectory);

    Version commitVersion = targetVersion;
    commitVersion.SyncSchemaVersionId(mSchema);
    commitVersion.SetOngoingModifyOperations(mOptions.GetOngoingModifyOperationIds());
    commitVersion.SetDescriptions(mOptions.GetVersionDescriptions());
    commitVersion.SetUpdateableSchemaStandards(mOptions.GetUpdateableSchemaStandards());
    VersionCommitter committer(rootDirectory, commitVersion, mKeepVersionCount, mOptions.GetReservedVersions());
    committer.Commit();
}

SegmentMergeInfos IndexPartitionMerger::CreateSegmentMergeInfos(const SegmentDirectoryPtr& segDir) const
{
    return MergeMetaCreator::CreateSegmentMergeInfos(mSchema, mMergeConfig, segDir);
}

void IndexPartitionMerger::ReclaimDocuments(const SegmentDirectoryPtr& segDir, int64_t currentTs)
{
    DocumentReclaimer docReclaimer(segDir, mSchema, mOptions, currentTs, mMetricProvider);
    docReclaimer.Reclaim();
    const Version& version = segDir->GetVersion();
    mDumpStrategy->Reload(version.GetTimestamp(), version.GetLocator());
}

MergeTask IndexPartitionMerger::CreateMergeTaskByStrategy(bool optimize, const MergeConfig& mergeConfig,
                                                          const SegmentMergeInfos& segMergeInfos,
                                                          const indexlibv2::framework::LevelInfo& levelInfo) const
{
    MergeStrategyPtr mergeStrategy = MergeStrategyFactory::GetInstance()->CreateMergeStrategy(
        mergeConfig.mergeStrategyStr, mSegmentDirectory, mSchema);
    mergeStrategy->SetParameter(mergeConfig.mergeStrategyParameter);
    if (!optimize) {
        return mergeStrategy->CreateMergeTask(segMergeInfos, levelInfo);
    } else {
        return mergeStrategy->CreateMergeTaskForOptimize(segMergeInfos, levelInfo);
    }
}

MergeSchedulerPtr IndexPartitionMerger::CreateMergeScheduler(int64_t maxMemUse, uint32_t maxThreadNum)
{
    MergeSchedulerPtr scheduler;
    scheduler.reset(new MultiThreadedMergeScheduler(maxMemUse, maxThreadNum));
    return scheduler;
}

std::string IndexPartitionMerger::GetMergeIndexRoot() const { return mDumpStrategy->GetRootPath(); }

file_system::DirectoryPtr IndexPartitionMerger::GetMergeIndexRootDirectory() const
{
    return mDumpStrategy->GetRootDirectory();
}

std::string IndexPartitionMerger::GetMergeMetaDir() const
{
    return util::PathUtil::JoinPath(mDumpStrategy->GetRootPath(), "merge_meta");
}

std::string IndexPartitionMerger::GetMergeCheckpointDir() const
{
    return util::PathUtil::JoinPath(mDumpStrategy->GetRootPath(), "checkpoints");
}

versionid_t IndexPartitionMerger::GetTargetVersionId(const Version& version, versionid_t alignVersionId)
{
    versionid_t lastVersionId = version.GetVersionId();
    if (alignVersionId == INVALID_VERSION) {
        // when full index no data, create empty version
        return lastVersionId == INVALID_VERSION ? 0 : lastVersionId;
    }

    if (lastVersionId > alignVersionId) {
        IE_LOG(ERROR, "align version id from [%d] to [%d] fail, alignVersion is smaller", lastVersionId,
               alignVersionId);
        return lastVersionId;
    }

    if (lastVersionId < alignVersionId) {
        IE_LOG(INFO, "align version id from [%d] to [%d]", lastVersionId, alignVersionId);
        return alignVersionId;
    }

    return lastVersionId;
}

void IndexPartitionMerger::AlignVersion(const file_system::DirectoryPtr& rootDirectory, versionid_t alignVersionId)
{
    Version latestVersion;
    VersionLoader::GetVersion(rootDirectory, latestVersion, INVALID_VERSION);
    versionid_t targetVersionId = GetTargetVersionId(latestVersion, alignVersionId);
    auto res = rootDirectory->GetFileSystem()->GetPhysicalPath(latestVersion.GetVersionFileName());
    string lastVersionPath = res.OK() ? res.result : "";

    if (latestVersion.GetVersionId() != targetVersionId || BranchFS::IsBranchPath(lastVersionPath)) {
        if (latestVersion.GetVersionId() == INVALID_VERSION) {
            // init empty version level info
            indexlibv2::framework::LevelInfo& levelInfo = latestVersion.GetLevelInfo();
            const BuildConfig& buildConfig = mOptions.GetBuildConfig();
            levelInfo.Init(buildConfig.levelTopology, buildConfig.levelNum, buildConfig.shardingColumnNum);
            latestVersion.SetTimestamp(mSegmentDirectory->GetVersion().GetTimestamp());
        }
        latestVersion.SetVersionId(targetVersionId);
        latestVersion.SyncSchemaVersionId(mSchema);
        latestVersion.SetOngoingModifyOperations(mOptions.GetOngoingModifyOperationIds());
        latestVersion.SetDescriptions(mOptions.GetVersionDescriptions());
        latestVersion.SetUpdateableSchemaStandards(mOptions.GetUpdateableSchemaStandards());
        VersionCommitter committer(rootDirectory, latestVersion, mKeepVersionCount, mOptions.GetReservedVersions());
        committer.Commit();
    }
}

void IndexPartitionMerger::TEST_MERGE_WITH_CHECKPOINTS(bool optimize, const MergeMetaPtr& meta, uint32_t instanceId)
{
    // remove check points, will do merge when index dirs exist
    string instDir = GetInstanceMergeDir(mDumpStrategy->GetRootPath(), instanceId);
    string checkPointDir = PathUtil::JoinPath(instDir, MERGE_ITEM_CHECK_POINT_DIR_NAME);
    auto ec = FslibWrapper::DeleteDir(checkPointDir, DeleteOption::NoFence(false)).Code();
    INDEXLIB_FATAL_ERROR_IF(ec != FSEC_OK && ec != FSEC_NOENT, FileIO, "DeleteDir Failed");
    DoMerge(optimize, meta, instanceId);

    // with check points, will skip merge by check points
    DoMerge(optimize, meta, instanceId);
}

void IndexPartitionMerger::PrepareMerge(int64_t currentTs)
{
    if (!DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, mSegmentDirectory)) {
        // TODO: add case
        // inc merge remove useless lost segments which segment id bigger than lastSegId in version
        // avoid useless segment effect build phase (worker degrade scene)
        DirectoryPtr rootDir = mSegmentDirectory->GetPartitionData()->GetRootDirectory();
        OfflineRecoverStrategy::Recover(mSegmentDirectory->GetVersion(), rootDir,
                                        config::RecoverStrategyType::RST_VERSION_LEVEL);

        // remove instance dirs using fence option (thanks for no backup in begin merge step)
        FenceContextPtr fenceContext = mDumpStrategy->GetRootDirectory()->GetFenceContext();
        for (const string& instanceName : ListInstanceDirNames(mDumpStrategy->GetRootPath())) {
            string instanceRoot = PathUtil::JoinPath(mDumpStrategy->GetRootPath(), instanceName);
            auto ec = FslibWrapper::DeleteDir(instanceRoot, DeleteOption::Fence(fenceContext.get(), false)).Code();
            THROW_IF_FS_ERROR(ec, "clean instance dir failed, [%s]", instanceRoot.c_str());
        }
    }

    IE_LOG(INFO, "begin reclaim documents");
    ReclaimDocuments(mSegmentDirectory, currentTs);
    IE_LOG(INFO, "reclaim documents done");
}

bool IndexPartitionMerger::IsTargetVersionValid(versionid_t versionId)
{
    Version version;
    try {
        VersionLoader::GetVersionS(mDumpStrategy->GetRootPath(), version, versionId);
    } catch (...) {
        return false;
    }
    if (version.GetVersionId() == INVALID_VERSION) {
        return false;
    }
    return true;
}

void IndexPartitionMerger::CommitToDefaultBranch(uint32_t instanceId) const
{
    mBranchFs->UpdatePreloadDependence();
    mBranchFs->CommitToDefaultBranch(mHinter.get());
}

void IndexPartitionMerger::SetCheckpointRootDir(const std::string& checkpointRoot)
{
    mCheckpointRootDir = checkpointRoot;
}

string IndexPartitionMerger::GetCheckpointDirForPhase(const string& phase) const
{
    if (mCheckpointRootDir.empty()) {
        return string("");
    }
    string dir = util::PathUtil::JoinPath(mCheckpointRootDir, phase);
    auto ret = file_system::FslibWrapper::MkDirIfNotExist(dir).Code();
    if (ret != file_system::FSEC_OK && ret != file_system::FSEC_EXIST) {
        INDEXLIB_FATAL_ERROR(FileIO, "make dir if not exist for [%s] error.", dir.c_str());
    }
    return dir;
}

}} // namespace indexlib::merger
