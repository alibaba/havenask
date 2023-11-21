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
#include "build_service/task_base/MergeTask.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <ostream>
#include <stddef.h>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/task_base/JobConfig.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/build_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/DeleteOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/common_branch_hinter.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/log.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/partition_group_resource.h"
#include "indexlib/partition/partition_validater.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::config;
using namespace autil;

using namespace indexlib::index_base;
using namespace indexlib::merger;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, MergeTask);
const string MergeTask::TRASH_DIR_NAME = "trash";
const uint32_t MergeTask::RESERVED_VERSION_NUM = 5;

MergeTask::MergeTask(uint32_t backupId, const string& epochId) : MergeTask(nullptr, backupId, epochId) {}

MergeTask::MergeTask(indexlib::util::MetricProviderPtr metricProvider, uint32_t backupId, const std::string& epochId)
    : TaskBase(metricProvider, epochId)
    , _backupId(backupId)
{
}

MergeTask::~MergeTask() {}

bool MergeTask::run(const string& jobParam, uint32_t instanceId, Mode mode, bool optimize)
{
    if (!TaskBase::init(jobParam)) {
        return false;
    }

    // COUNTER_CONFIG_JSON_STR will not be set in bs_local_job
    CounterConfigPtr counterConfig;
    auto it = _kvMap.find(COUNTER_CONFIG_JSON_STR);
    if (it == _kvMap.end()) {
        BS_LOG(ERROR, "[%s] missing in jobParam", COUNTER_CONFIG_JSON_STR.c_str());
    } else {
        counterConfig.reset(new CounterConfig());
        try {
            FromJsonString(*counterConfig.get(), it->second);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "jsonize [%s] failed, original str : [%s]", COUNTER_CONFIG_JSON_STR.c_str(),
                   it->second.c_str());
            return false;
        }
    }

    pair<MergeStep, uint32_t> mergeStep = getMergeStep(mode, instanceId);
    if (mergeStep.first == MS_DO_NOTHING) {
        return true;
    }
    PartitionId partitionId = createPartitionId(instanceId, mode, "merger");
    BS_LOG(INFO, "instanceId[%u] %s backupId[%d], step[%d]", instanceId, partitionId.ShortDebugString().c_str(),
           _backupId, mergeStep.first);
    if (!_metricProvider && !startMonitor(partitionId.range(), mode)) {
        string errorMsg = "start monitor failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    string indexRoot = getValueFromKeyValueMap(_kvMap, INDEX_ROOT_PATH);
    string targetIndexPath = IndexPathConstructor::constructIndexPath(indexRoot, partitionId);
    _fenceContext.reset(FslibWrapper::CreateFenceContext(targetIndexPath, _epochId));
    bool needMergeMultiPart = isMultiPartMerge();
    try {
        if (needMergeMultiPart) {
            string builderIndexRoot = getValueFromKeyValueMap(_kvMap, BUILDER_INDEX_ROOT_PATH, indexRoot);
            return mergeMultiPart(partitionId, builderIndexRoot, targetIndexPath, _mergeIndexPartitionOptions,
                                  mergeStep, optimize, counterConfig);
        } else {
            return mergeSinglePart(partitionId, targetIndexPath, _mergeIndexPartitionOptions, mergeStep, optimize,
                                   counterConfig);
        }
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "merge caught autil exception [%s]", e.what());
        return false;
    } catch (const std::exception& e) {
        BS_LOG(ERROR, "merge caught std::exception[%s]", e.what());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "%s", "unknown exception");
        return false;
    }
}

bool MergeTask::mergeMultiPart(const PartitionId& partitionId, const string& indexRoot, const string& targetIndexPath,
                               const IndexPartitionOptions& options, const pair<MergeStep, uint32_t> mergeStep,
                               bool optimize, const CounterConfigPtr& counterConfig)
{
    vector<Range> mergePartRanges = _jobConfig.getAllNeedMergePart(partitionId.range());
    vector<string> mergePartPaths;
    mergePartPaths.reserve(mergePartRanges.size());
    for (size_t i = 0; i < mergePartRanges.size(); ++i) {
        PartitionId toMergePartId = partitionId;
        *toMergePartId.mutable_range() = mergePartRanges[i];
        mergePartPaths.push_back(IndexPathConstructor::constructIndexPath(indexRoot, toMergePartId));
    }
    const string& clusterName = _jobConfig.getClusterName();
    auto schema = _resourceReader->getSchema(clusterName);
    if (!schema) {
        IE_LOG(ERROR, "get schema for cluster[%s] failed", clusterName.c_str());
        return false;
    }
    IndexPartitionMergerPtr merger;
    indexlib::PartitionRange targetRange(partitionId.range().from(), partitionId.range().to());
    try {
        merger.reset((IndexPartitionMerger*)PartitionMergerCreator::CreateFullParallelPartitionMerger(
            mergePartPaths, targetIndexPath, options, _metricProvider, _resourceReader->getPluginPath(), targetRange,
            CommonBranchHinterOption::Normal(_backupId, _epochId)));
    } catch (const ExceptionBase& e) {
        string errorMsg = "Create merger failed! error:[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    CounterSynchronizerPtr counterSynchronizer;
    if (mergeStep.first == MS_DO_MERGE && counterConfig) {
        counterSynchronizer.reset(CounterSynchronizerCreator::create(merger->GetCounterMap(), counterConfig));

        if (!counterSynchronizer) {
            BS_LOG(ERROR, "init counterSynchronizer failed");
        } else {
            if (!counterSynchronizer->beginSync()) {
                BS_LOG(ERROR, "create sync counter thread failed");
            }
        }
    }

    if (!doMerge(merger, mergeStep, optimize, partitionId)) {
        return false;
    }
    if (mergeStep.first == MS_END_MERGE) {
        for (size_t i = 0; i < mergePartPaths.size(); ++i) {
            _uselessPaths.push_back(mergePartPaths[i]);
        }
    }
    return true;
}

void MergeTask::cleanUselessResource()
{
    for (size_t i = 0; i < _uselessPaths.size(); ++i) {
        auto ec = FslibWrapper::DeleteFile(_uselessPaths[i], DeleteOption::Fence(_fenceContext.get(), true)).Code();
        if (ec == FSEC_OK) {
            BS_LOG(INFO, "successfully remove dir[%s]", _uselessPaths[i].c_str());
        } else {
            string errorMsg = "failed to remove dir[" + _uselessPaths[i] + "]";
            BS_LOG(ERROR, "%s, ec [%d]", errorMsg.c_str(), ec);
        }
    }
}

bool MergeTask::mergeSinglePart(const PartitionId& partitionId, const string& targetIndexPath,
                                const IndexPartitionOptions& options, const pair<MergeStep, uint32_t> mergeStep,
                                bool optimize, const CounterConfigPtr& counterConfig)
{
    IndexPartitionMergerPtr merger = createSinglePartMerger(partitionId, targetIndexPath, options);
    if (!merger) {
        return false;
    }

    CounterSynchronizerPtr counterSynchronizer;
    if (mergeStep.first == MS_DO_MERGE && counterConfig) {
        counterSynchronizer.reset(CounterSynchronizerCreator::create(merger->GetCounterMap(), counterConfig));

        if (!counterSynchronizer) {
            BS_LOG(ERROR, "init counterSynchronizer failed");
        } else {
            if (!counterSynchronizer->beginSync()) {
                BS_LOG(ERROR, "create sync counter thread failed");
            }
        }
    }

    if (!doMerge(merger, mergeStep, optimize, partitionId)) {
        string errorMsg = "merge failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    DirectoryPtr targetDirectory =
        Directory::ConstructByFenceContext(Directory::GetPhysicalDirectory(targetIndexPath), _fenceContext, nullptr);
    VersionCommitter::CleanVersions(targetDirectory, options.GetBuildConfig().keepVersionCount,
                                    options.GetReservedVersions());

    return cleanFullStepUselessFile(mergeStep, targetIndexPath);
}

bool MergeTask::cleanFullStepUselessFile(const pair<MergeStep, uint32_t> mergeStep, const std::string& targetIndexPath)
{
    BuildStep buildStep = BUILD_STEP_FULL;
    if (mergeStep.first == MS_END_MERGE && getBuildStep(buildStep) && buildStep == BUILD_STEP_FULL) {
        _uselessPaths.push_back(fslib::util::FileUtil::joinFilePath(targetIndexPath, MARKED_BRANCH_INFO));
        // clear root branch path as it's builder produce, not need for keep
        vector<string> fileList;
        if (fslib::util::FileUtil::listDir(targetIndexPath, fileList)) {
            for (auto file : fileList) {
                if (BranchFS::IsBranchPath(file) &&
                    EpochIdUtil::CompareGE(_epochId, CommonBranchHinter::ExtractEpochFromBranch(file))) {
                    _uselessPaths.push_back(fslib::util::FileUtil::joinFilePath(targetIndexPath, file));
                }
            }
        } else {
            BS_LOG(ERROR, "list dir [%s] failed, cannot clear useless file", targetIndexPath.c_str());
            return false;
        }
    }
    return true;
}

IndexPartitionMergerPtr MergeTask::createSinglePartMerger(const PartitionId& partitionId, const string& targetIndexPath,
                                                          const IndexPartitionOptions& options)
{
    int32_t workerPathVersion = -1;
    BuildStep buildStep = BUILD_STEP_FULL;
    if (!getWorkerPathVersion(workerPathVersion)) {
        return IndexPartitionMergerPtr();
    }
    if (!getBuildStep(buildStep)) {
        return IndexPartitionMergerPtr();
    }

    const string& clusterName = _jobConfig.getClusterName();
    auto schema = _resourceReader->getSchema(clusterName);
    if (!schema) {
        BS_LOG(ERROR, "get schema failed for cluster[%s]", clusterName.c_str());
        return IndexPartitionMergerPtr();
    }

    indexlib::PartitionRange targetRange(partitionId.range().from(), partitionId.range().to());
    try {
        IndexPartitionMergerPtr merger;
        if (buildStep == BUILD_STEP_FULL || workerPathVersion < 0) {
            merger.reset((IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
                targetIndexPath, options, _metricProvider, _resourceReader->getPluginPath(), targetRange,
                CommonBranchHinterOption::Normal(_backupId, _epochId)));
        } else {
            merger = CreateIncParallelPartitionMerger(partitionId, targetIndexPath, options, workerPathVersion);
        }
        return merger;
    } catch (const ExceptionBase& e) {
        string errorMsg = "create merger for merge failed! exception[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return IndexPartitionMergerPtr();
    }
}

bool MergeTask::isMultiPartMerge() const
{
    string indexRoot = getValueFromKeyValueMap(_kvMap, INDEX_ROOT_PATH);
    string builderIndexRoot = getValueFromKeyValueMap(_kvMap, BUILDER_INDEX_ROOT_PATH, "");
    if (!builderIndexRoot.empty() && indexRoot != builderIndexRoot) {
        return true;
    }
    const BuildRuleConfig& buildRuleConf = _jobConfig.getBuildRuleConf();
    uint32_t originBuildParallelNum = buildRuleConf.buildParallelNum / buildRuleConf.partitionSplitNum;
    assert(originBuildParallelNum * buildRuleConf.partitionSplitNum == buildRuleConf.buildParallelNum);
    return originBuildParallelNum != 1;
}

pair<MergeTask::MergeStep, uint32_t> MergeTask::getMergeStep(Mode mode, uint32_t instanceId) const
{
    const BuildRuleConfig& buildRuleConf = _jobConfig.getBuildRuleConf();
    // reinit instanceId here
    if (mode == MERGE_MAP) {
        return make_pair(MS_INIT_MERGE, uint32_t(0));
    } else if (mode == MERGE_REDUCE) {
        uint32_t mergeReduceInstanceId = instanceId % buildRuleConf.mergeParallelNum;
        return make_pair(MS_DO_MERGE, mergeReduceInstanceId);
    } else if (mode == END_MERGE_MAP) {
        return make_pair(MS_END_MERGE, uint32_t(0));
    } else {
        return make_pair(MS_DO_NOTHING, uint32_t(0));
    }
}

string MergeTask::timeDiffToString(int64_t timeDiff)
{
    stringstream ss;
    ss << timeDiff / 3600 << " h " << (timeDiff % 3600) / 60 << " m " << (timeDiff % 60) << " s ";
    return ss.str();
}

void MergeTask::removeObsoleteMergeDir(const string& mergeMetaRoot, FenceContext* fenceContext,
                                       uint32_t reservedVersionNum)
{
    vector<string> fileList;
    if (!fslib::util::FileUtil::listDir(mergeMetaRoot, fileList)) {
        BS_LOG(INFO, "list dir [%s] failed", mergeMetaRoot.c_str());
        return;
    }
    if (fileList.size() <= reservedVersionNum) {
        return;
    }
    sort(fileList.begin(), fileList.end());
    for (size_t i = 0; i < fileList.size() - reservedVersionNum; i++) {
        if (!FslibWrapper::DeleteDir(fslib::util::FileUtil::joinFilePath(mergeMetaRoot, fileList[i]),
                                     DeleteOption::Fence(fenceContext, true))
                 .OK()) {
            BS_LOG(WARN, "remove dir [%s] in root [%s] failed", fileList[i].c_str(), mergeMetaRoot.c_str());
        }
    }
}

bool MergeTask::doMerge(IndexPartitionMergerPtr& indexPartMerger, const pair<MergeStep, uint32_t>& mergeStep,
                        bool optimize, const PartitionId& partitionId)
{
    std::string targetIndexPath = indexPartMerger->GetMergeIndexRoot();
    const BuildRuleConfig& buildRuleConf = _jobConfig.getBuildRuleConf();
    uint32_t totalInstanceCount = buildRuleConf.mergeParallelNum;
    string mergeMetaRoot = indexPartMerger->GetMergeMetaDir();
    string checkpointRoot = indexPartMerger->GetMergeCheckpointDir();

    if (_kvMap.find(MERGE_TIMESTAMP) == _kvMap.end()) {
        BS_LOG(ERROR, "Can not find MERGE_TIMESTAMP in kvMap!");
        return false;
    }
    string mergeMetaVersionDir = fslib::util::FileUtil::joinFilePath(mergeMetaRoot, _kvMap[MERGE_TIMESTAMP]);
    BS_LOG(INFO, "mergeMetaVersionDir: %s", mergeMetaVersionDir.c_str());

    string checkpointVersionDir = fslib::util::FileUtil::joinFilePath(checkpointRoot, _kvMap[MERGE_TIMESTAMP]);
    indexPartMerger->SetCheckpointRootDir(checkpointVersionDir);

    string roleId;
    ProtoUtil::partitionIdToRoleId(partitionId, roleId);

    FenceContextPtr fenceContext = indexPartMerger->GetMergeIndexRootDirectory()->GetFenceContext();
    try {
        switch (mergeStep.first) {
        case MS_INIT_MERGE: {
            if (_backupId) {
                BS_LOG(ERROR, "INIT_MERGE does not support backup node");
                return false;
            }
            int64_t curTs = TimeUtility::currentTimeInMicroSeconds();
            BS_LOG(INFO, "CreateMergeMeta with current ts [%ld]", curTs);
            indexPartMerger->PrepareMerge(curTs);
            MergeMetaPtr mergeMeta = indexPartMerger->CreateMergeMeta(optimize, totalInstanceCount, curTs);
            mergeMeta->Store(mergeMetaVersionDir, fenceContext.get());
            _mergeStatus.isMergeFinished = true;
            versionid_t targetVersionId = mergeMeta->GetTargetVersion().GetVersionId();
            if (targetVersionId == indexlib::INVALID_VERSIONID) {
                Version onDiskVersion;
                VersionLoader::GetVersion(indexPartMerger->GetMergeIndexRootDirectory(), onDiskVersion,
                                          indexlib::INVALID_VERSIONID);
                targetVersionId = onDiskVersion.GetVersionId();
            }
            _mergeStatus.targetVersionId = targetVersionId;
            break;
        }
        case MS_DO_MERGE: {
            MergeMetaPtr mergeMeta = indexPartMerger->LoadMergeMeta(mergeMetaVersionDir, false);
            if (!mergeMeta) {
                string errorMsg = "Load merge meta file: [" + mergeMetaVersionDir + "] FAILED.";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            indexPartMerger->DoMerge(optimize, mergeMeta, mergeStep.second);
            break;
        }
        case MS_END_MERGE: {
            if (_backupId) {
                BS_LOG(ERROR, "END_MERGE does not support backup node");
                return false;
            }
            MergeMetaPtr mergeMeta = indexPartMerger->LoadMergeMeta(mergeMetaVersionDir, true);
            if (!mergeMeta) {
                string errorMsg = "Load merge meta file: [" + mergeMetaVersionDir + "] FAILED.";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            string alignedVersionId = getValueFromKeyValueMap(_kvMap, ALIGNED_VERSION_ID, "-1");
            int32_t versionId = indexlib::INVALID_VERSIONID;
            if (!StringUtil::numberFromString(alignedVersionId, versionId)) {
                BS_LOG(ERROR, "invalid aligned version id: %s", alignedVersionId.c_str());
                return false;
            }
            indexPartMerger->EndMerge(mergeMeta, versionId);
            ValidateLatestIndexVersion(targetIndexPath, versionId);
            removeObsoleteMergeDir(mergeMetaRoot, fenceContext.get());
            removeObsoleteMergeDir(checkpointRoot, fenceContext.get());

            // TODO: change target version id
            _mergeStatus.isMergeFinished = true;
            _mergeStatus.targetVersionId = versionId;
            break;
        }
        default:
            break;
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        string errorMsg = "Merge fail, exception " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        return false;
    } catch (...) {
        string errorMsg = "Merge fail, unknown exception";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        return false;
    }

    return true;
}

IndexPartitionMergerPtr MergeTask::CreateIncParallelPartitionMerger(const proto::PartitionId& partitionId,
                                                                    const string& targetIndexPath,
                                                                    const IndexPartitionOptions& options,
                                                                    uint32_t workerPathVersion)
{
    // uint32_t backupId = 0;
    // if (partitionId.has_backupid() && partitionId.backupid() != 0) {
    //     backupId = partitionId.backupid();
    //     BS_LOG(INFO, "Prepare backup merger of backupId [%d]", backupId);
    // }
    ParallelBuildInfo incBuildInfo;
    incBuildInfo.batchId = workerPathVersion;
    uint32_t parallelNum = 0;
    if (!getIncBuildParallelNum(parallelNum)) {
        return IndexPartitionMergerPtr();
    }
    incBuildInfo.parallelNum = parallelNum;
    // merger do not care instanceId
    incBuildInfo.instanceId = 0;
    indexlib::PartitionRange targetRange(partitionId.range().from(), partitionId.range().to());
    IndexPartitionMergerPtr merger((IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
        targetIndexPath, incBuildInfo, options, _metricProvider, _resourceReader->getPluginPath(), targetRange,
        CommonBranchHinterOption::Normal(_backupId, _epochId)));
    return merger;
}

void MergeTask::ValidateLatestIndexVersion(const std::string& targetIndexPath, versionid_t targetVersionId)
{
    bool needValidate = autil::EnvUtil::getEnv("VALIDATE_INDEX_AFTER_MERGE", false);
    if (!needValidate) {
        return;
    }

    BS_LOG(INFO, "Begin validate target version [%d] in path [%s].", targetVersionId, targetIndexPath.c_str());
    indexlib::partition::PartitionValidater validater(_mergeIndexPartitionOptions);
    if (!validater.Init(targetIndexPath, targetVersionId)) {
        AUTIL_LEGACY_THROW(indexlib::util::IndexCollapsedException,
                           "Init PartitionValidater for path [" + targetIndexPath + "], version [" +
                               autil::StringUtil::toString(targetVersionId) + "] FAILED.");
    }
    validater.Check();
    BS_LOG(INFO, "Finish validate target version [%d] in path [%s].", targetVersionId, targetIndexPath.c_str());
}

}} // namespace build_service::task_base
