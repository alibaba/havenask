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
#include "build_service/admin/taskcontroller/SingleMergerTask.h"

#include <assert.h>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "beeper/common/common_type.h"
#include "build_service/admin/AlterFieldCKPAccessor.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/admin/CheckpointTools.h"
#include "build_service/admin/SlowNodeDetector.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/IndexPartitionOptionsWrapper.h"
#include "build_service/config/OfflineMergeConfig.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/build_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::util;

using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointAccessor;
using build_service::common::IndexCheckpointAccessorPtr;
using build_service::common::IndexCheckpointFormatter;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, SingleMergerTask);

int32_t SingleMergerTask::DEFAULT_CHECKPOINT_KEEP_COUNT = 1;
string SingleMergerTask::MERGE_CONFIG_SYNC_VERSION = ALIGN_VERSION_MERGE_STRATEGY_STR;

SingleMergerTask::SingleMergerTask(const proto::BuildId& buildId, const string& clusterName,
                                   const TaskResourceManagerPtr& resMgr)
    : AdminTaskBase(buildId, resMgr)
    , _optimizeMerge(true)
    , _mergeStep(MS_BEGIN_MERGE)
    , _clusterName(clusterName)
    , _partitionCount(0)
    , _buildParallelNum(1)
    , _mergeParallelNum(1)
    , _workerPathVersion(-1)
    , _timestamp(0)
    , _alignedVersionId(indexlib::INVALID_VERSIONID)
    , _alignVersion(true)
    , _schemaVersion(0)
    , _checkpointKeepCount(DEFAULT_CHECKPOINT_KEEP_COUNT)
    , _buildStep(BUILD_STEP_FULL)
    , _remainIndexSize(-1)
    , _needWaitAlterField(true)
    , _disableFillIndexInfo(false)
    , _maxOpId(INVALID_SCHEMA_OP_ID)
    , _workerProtocolVersion(UNKNOWN_WORKER_PROTOCOL_VERSION)
    , _batchMask(-1)
{
    _indexCkpAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    _builderCkpAccessor = CheckpointCreator::createBuilderCheckpointAccessor(_resourceManager);
}

SingleMergerTask::~SingleMergerTask() {}

bool SingleMergerTask::startFromCheckpoint(int64_t schemaId, bool skipBuildCheckpoint)
{
    config::ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);

    _workerProtocolVersion = configAccessor->getLatestConfig()->getWorkerProtocolVersion();
    BuilderCheckpoint builderCheckpoint;
    if (!getLatestCheckpoint(builderCheckpoint)) {
        if (skipBuildCheckpoint) {
            _builderCheckpoint = IndexCheckpointFormatter::encodeBuilderCheckpoint(builderCheckpoint);
            if (schemaId == -1) {
                auto lastestReader = configAccessor->getLatestConfig();
                if (!lastestReader) {
                    BS_LOG(ERROR, "cannot get lastest reader for buildId [%s]", _buildId.ShortDebugString().c_str());
                    return false;
                }
                auto schema = lastestReader->getSchema(_clusterName);
                schemaId = schema->GetSchemaVersionId();
            }
            builderCheckpoint.set_workerpathversion(0);
        } else {
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "start merger failed: cannot get builder checkpoint",
                          *_beeperTags);
            return false;
        }
    }
    config::ResourceReaderPtr resourceReader;
    if (schemaId != -1) {
        resourceReader = configAccessor->getConfig(_clusterName, schemaId);
    } else {
        resourceReader = configAccessor->getConfig(_clusterName, builderCheckpoint.schemaversion());
    }
    if (!resourceReader) {
        BS_LOG(ERROR, "no config reader found");
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start merger failed: no config reader found, schemaId[%ld]", schemaId);
        return false;
    }
    if (!loadFromConfig(resourceReader)) {
        return false;
    }
    if (_buildStep == BUILD_STEP_INC) {
        switchToInc();
    }
    prepareMerge(builderCheckpoint.workerpathversion());
    return true;
}

bool SingleMergerTask::getLatestCheckpoint(BuilderCheckpoint& builderCheckpoint)
{
    if (!_builderCkpAccessor->getLatestCheckpoint(_clusterName, builderCheckpoint, _builderCheckpoint)) {
        BS_LOG(ERROR, "no builder checkpoint, can not start merger");
        return false;
    }
    return true;
}

bool SingleMergerTask::init(proto::BuildStep buildStep, const string& mergeConfigName, int32_t batchMask)
{
    _buildStep = buildStep;
    _mergeConfigName = mergeConfigName;
    _batchMask = batchMask;
    return true;
}

bool SingleMergerTask::start(const KeyValueMap& kvMap)
{
    auto iter = kvMap.find("mergeConfigName");
    if (iter != kvMap.end()) {
        _mergeConfigName = iter->second;
    }
    BS_LOG(INFO, "use mergeConfigName [%s] from start parameter", _mergeConfigName.c_str());
    if (getValueFromKeyValueMap(kvMap, "disableFillIndexInfo") == "true") {
        _disableFillIndexInfo = true;
    } else {
        _disableFillIndexInfo = false;
    }
    int64_t schemaId = config::INVALID_SCHEMAVERSION;
    iter = kvMap.find("schemaId");
    if (iter != kvMap.end()) {
        if (!StringUtil::numberFromString(iter->second, schemaId)) {
            BS_LOG(ERROR, "invalid schema id[%s]", iter->second.c_str());
            BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                                 "start merger failed: invalid schema id[%s]", iter->second.c_str());
            return false;
        }
    }
    bool skipBuildCheckpoint = false;
    iter = kvMap.find("skipBuildCheckpoint");
    if (iter != kvMap.end() && iter->second == "true") {
        skipBuildCheckpoint = true;
        BS_LOG(INFO, "set skip build checkpoint true for buildId [%s]", _buildId.ShortDebugString().c_str());
    }
    if (!startFromCheckpoint(schemaId, skipBuildCheckpoint)) {
        BS_LOG(ERROR, "start merger[%s] failed.", _clusterName.c_str());
        return false;
    }
    _taskStatus = TASK_NORMAL;
    bool needWait = true;
    if (!getMergeConfigNeedWaitAlterField(needWait)) {
        BS_LOG(ERROR, "start merger[%s] failed.", _clusterName.c_str());
        return false;
    }
    if (needWaitAlterField(needWait)) {
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start merger success, mergeConfigName[%s], schemaId[%ld]", _mergeConfigName.c_str(),
                             _schemaVersion);
        if (_beeperTags) {
            _beeperTags->AddTag("mergeConfigName", _mergeConfigName);
        }
        return true;
    }
    return false;
}

bool SingleMergerTask::getMergeConfigNeedWaitAlterField(bool& needWait) const
{
    if (_mergeConfigName == ALIGN_VERSION_MERGE_STRATEGY_STR) {
        needWait = false;
        return true;
    }
    config::ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    ResourceReaderPtr resourceReader = configAccessor->getConfig(_configPath);
    string key = "offline_index_config.customized_merge_config." + _mergeConfigName;
    OfflineMergeConfig mergeConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, key, mergeConfig)) {
        BS_LOG(ERROR, "get %s failed", key.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start merger failed: get %s from %s failed", key.c_str(), _configPath.c_str());
        return false;
    }
    needWait = mergeConfig.needWaitAlterField;
    return true;
}

bool SingleMergerTask::needWaitAlterField(bool mergeConfigNeedWaitAlterField)
{
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    vector<schema_opid_t> ongoingOpIds;
    if (!AlterFieldCKPAccessor::getOngoingOpIds(checkpointAccessor, _clusterName, _maxOpId, ongoingOpIds)) {
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "start merger failed, cannot get alter filed checkpoint",
                      *_beeperTags);
        return false;
    }
    if (ongoingOpIds.empty()) {
        vector<schema_opid_t> disableOpIds;
        if (!CheckpointTools::getDisableOpIds(checkpointAccessor, _clusterName, disableOpIds, _maxOpId)) {
            BS_LOG(ERROR, "get disable opids failed, cluster[%s]", _clusterName.c_str());
            return true;
        }
        _opIds = StringUtil::toString(disableOpIds, ",");
        _needWaitAlterField = false;
        return true;
    }
    // if current merge config need to wait all operations ready
    if (mergeConfigNeedWaitAlterField) {
        _needWaitAlterField = true;
        return true;
    }
    // if not need to wait all operations ready, do sync version
    _mergeConfigName = MERGE_CONFIG_SYNC_VERSION;
    _needWaitAlterField = false;
    return true;
}

void SingleMergerTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("has_create_nodes", _hasCreateNodes, _hasCreateNodes);
    json.Jsonize("optimize_merge", _optimizeMerge);
    json.Jsonize("merge_step", _mergeStep);
    json.Jsonize("config_path", _configPath);
    json.Jsonize("partition_count", _partitionCount);
    json.Jsonize("build_parallel_num", _buildParallelNum);
    json.Jsonize("merge_parallel_num", _mergeParallelNum);
    json.Jsonize("merge_config_name", _mergeConfigName);
    json.Jsonize("timestamp", _timestamp);
    json.Jsonize("aligned_version_id", _alignedVersionId, _alignedVersionId);
    json.Jsonize("align_version", _alignVersion, _alignVersion);
    json.Jsonize("slow_node_detect", _slowNodeDetect, false);
    json.Jsonize("nodes_start_timestamp", _nodesStartTimestamp, -1L);
    json.Jsonize("schem_version", _schemaVersion, _schemaVersion);
    json.Jsonize("worker_path_version", _workerPathVersion, _workerPathVersion);
    json.Jsonize("keep_checkpoint_count", _checkpointKeepCount, _checkpointKeepCount);
    json.Jsonize("builder_checkpoint", _builderCheckpoint, _builderCheckpoint);
    json.Jsonize("build_step", _buildStep, _buildStep);
    json.Jsonize("remain_index_size", _remainIndexSize, _remainIndexSize);
    json.Jsonize("finish_step_info", _finishStepString, _finishStepString);
    json.Jsonize("need_wait_alter_field", _needWaitAlterField, _needWaitAlterField);
    json.Jsonize("disable_fill_index_info", _disableFillIndexInfo, _disableFillIndexInfo);
    json.Jsonize("max_opid", _maxOpId, _maxOpId);
    json.Jsonize("op_ids", _opIds, _opIds);
    json.Jsonize("worker_protocol_version", _workerProtocolVersion, _workerProtocolVersion);
    json.Jsonize(BATCH_MASK, _batchMask, _batchMask);
}

string SingleMergerTask::getTaskPhaseIdentifier() const { return string("merger_phase_") + MergeStep_Name(_mergeStep); }

void SingleMergerTask::doSupplementLableInfo(KeyValueMap& info) const
{
    info["optimize_merge"] = _optimizeMerge ? "true" : "false";
    info["merge_step"] = MergeStep_Name(_mergeStep);
    info["merge_config_name"] = _mergeConfigName;
    info["remain_index_size"] = StringUtil::toString(_remainIndexSize);
    info["schema_version"] = StringUtil::toString(_schemaVersion);

    if (!_builderCheckpoint.empty()) {
        BuilderCheckpoint builderCheckpoint;
        IndexCheckpointFormatter::decodeBuilderCheckpoint(_builderCheckpoint, builderCheckpoint);
        info["version_timestamp"] = AdminTaskBase::getDateFormatString(builderCheckpoint.versiontimestamp());
        info["processor_checkpoint"] = AdminTaskBase::getDateFormatString(builderCheckpoint.processorcheckpoint());
    }
    info["prepare_merge_timestamp"] = AdminTaskBase::getDateFormatString(_timestamp);
    info["aligned_version_id"] = _alignVersion ? StringUtil::toString(_alignedVersionId) : "";
    info["finish_steps"] = _finishStepString;
    if (_maxOpId != INVALID_SCHEMA_OP_ID) {
        info["max_modify_opid"] = StringUtil::toString(_maxOpId);
    }
    if (!_opIds.empty()) {
        info["ongoing_opids"] = _opIds;
    }
}

bool SingleMergerTask::initOpIds(uint32_t maxOpId)
{
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    vector<schema_opid_t> ongoingOpIds;
    if (!AlterFieldCKPAccessor::getOngoingOpIds(checkpointAccessor, _clusterName, maxOpId, ongoingOpIds)) {
        return false;
    }
    set<schema_opid_t> allOpIds(ongoingOpIds.begin(), ongoingOpIds.end());
    vector<schema_opid_t> disableOpIds;
    if (!CheckpointTools::getDisableOpIds(checkpointAccessor, _clusterName, disableOpIds)) {
        return false;
    }
    for (schema_opid_t opId : disableOpIds) {
        if (allOpIds.find(opId) == allOpIds.end() && opId <= maxOpId) {
            ongoingOpIds.push_back(opId);
        }
    }

    _opIds = StringUtil::toString(ongoingOpIds, ",");
    BS_LOG(INFO, "init opid[%s] for merger[%s]", _opIds.c_str(), _clusterName.c_str());
    return true;
}

bool SingleMergerTask::loadFromConfig(const ResourceReaderPtr& resourceReader)
{
    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        string errorMsg = "parse cluster_config.builder_rule_config for [" + _clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags, "start merger failed: %s",
                             errorMsg.c_str());
        return false;
    }
    _configPath = resourceReader->getOriginalConfigPath();
    _partitionCount = buildRuleConfig.partitionCount;
    _buildParallelNum = buildRuleConfig.buildParallelNum;
    _mergeParallelNum = 1;
    _alignVersion = buildRuleConfig.alignVersion;
    if (!updateKeepCheckpointCount(resourceReader)) {
        return false;
    }
    auto schema = resourceReader->getSchema(_clusterName);
    if (!schema) {
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start merger failed: get schema from %s failed", _configPath.c_str());
        return false;
    }
    _schemaVersion = (int64_t)schema->GetSchemaVersionId();
    _maxOpId = schema->GetModifyOperationCount();
    initOpIds(_maxOpId);
    return initSlowNodeDetect(resourceReader, _clusterName);
}

void SingleMergerTask::initMergeNodes(MergerNodes& activeNodes)
{
    if (_nodesStartTimestamp == -1) {
        _nodesStartTimestamp = autil::TimeUtility::currentTimeInMicroSeconds();
    }
    _slowNodeDetector.reset();

    generateAndSetTarget(activeNodes);
    _nodeStatusManager->Update(activeNodes);
    BS_LOG(INFO, "cluster(%s) merge(%s) begin", _clusterName.c_str(), _mergeConfigName.c_str());
}

void SingleMergerTask::waitSuspended(WorkerNodes& workerNodes)
{
    if (_taskStatus == TASK_SUSPENDED) {
        return;
    }
    MergerNodes mergerNodes;
    mergerNodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        mergerNodes.push_back(DYNAMIC_POINTER_CAST(MergerNode, workerNode));
    }
    if (doWaitSuspended(mergerNodes)) {
        _taskStatus = TASK_SUSPENDED;
    }
}

bool SingleMergerTask::run(WorkerNodes& workerNodes)
{
    MergerNodes mergerNodes;
    if (_beeperTags && _beeperTags->FindTag("mergeConfigName") == "") {
        _beeperTags->AddTag("mergeConfigName", _mergeConfigName);
    }
    mergerNodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        mergerNodes.push_back(DYNAMIC_POINTER_CAST(MergerNode, workerNode));
    }
    bool ret = run(mergerNodes);
    workerNodes.clear();
    workerNodes.reserve(mergerNodes.size());
    for (auto& mergerNode : mergerNodes) {
        workerNodes.push_back(mergerNode);
    }
    if (ret) {
        _taskStatus = TASK_FINISHED;
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "merger task finish, schemaVersion is [%ld]", _schemaVersion);
    }
    return ret;
}

bool SingleMergerTask::run(MergerNodes& activeNodes)
{
    if (_needWaitAlterField) {
        // if merge_config.need_wait_alter_field = true, and not all operations is ready
        // wait unit all operations is ready, _needWaitAlterField = false
        needWaitAlterField(true);
        return false;
    }

    if (activeNodes.empty() || checkConfigChanged(activeNodes)) {
        activeNodes.clear();
        _nodeStatusManager->Update(activeNodes);
        _slowNodeDetector.reset();
        uint32_t parallelNum = _mergeStep == MS_DO_MERGE ? _mergeParallelNum : 1;
        activeNodes =
            WorkerNodeCreator<ROLE_MERGER>::createNodes(_partitionCount, parallelNum, BUILD_STEP_FULL, /* not used */
                                                        _buildId, _clusterName, _mergeConfigName, getTaskIdentifier());
        recoverFromBackInfo(activeNodes);
        _hasCreateNodes = true;
        initMergeNodes(activeNodes);
        return false;
    }
    /*
        attention!!!
        checkAndSetFinished、 update activeNodes 、detectSlowNodes must be called follow the order below
    */
    bool isFinished = checkAndSetFinished(_nodeStatusManager, activeNodes,
                                          _slowNodeDetect ? &(_slowNodeDetector->getMetric()) : nullptr);
    _nodeStatusManager->Update(activeNodes);
    if (_mergeStep == MS_DO_MERGE) {
        detectSlowNodes(activeNodes, _clusterName);
    } else if (_mergeStep == MS_END_MERGE || _mergeStep == MS_BEGIN_MERGE) {
        detectSlowNodes(activeNodes, _clusterName, /*simpleHandle=*/true);
    }
    /*
        optimization for tiny table of igraph, do not reallocate hippo role during the whole procedure.
    */
    bool keepWorkerNodeAfterFinish = _mergeParallelNum == 1 && _mergeStep != MS_END_MERGE;
    for (auto& node : activeNodes) {
        node->setKeepAliveAfterFinish(keepWorkerNodeAfterFinish);
    }
    if (!isFinished) {
        saveBackupInfo();
        generateAndSetTarget(activeNodes);
        return false;
    }

    if (_mergeStep == MS_BEGIN_MERGE) {
        collectAlignVersion(activeNodes);
    }
    if (_mergeStep == MS_DO_MERGE) {
        /*
           Wait all role to be suspend or finished
           If a role belongs to a finished roleGroup that has another finished role in the same role group is not
           finished, it should receive a suspend command to make sure this role is drastically stopped, otherwise it may
           affect the build processes in the next phase.
        */
        if (!doWaitSuspended(activeNodes)) {
            BS_LOG(INFO, "wait all role to be suspended to enter next build phase...");
            saveBackupInfo();
            return false;
        }
    }
    if (_mergeStep == MS_END_MERGE) {
        ::google::protobuf::RepeatedPtrField<proto::IndexInfo> indexInfos;
        collectIndexInfo(activeNodes, indexInfos);
        _nodesStartTimestamp = -1;
        activeNodes.clear();
        clearBackupInfo();
        _nodeStatusManager->Update(activeNodes);
        endMerge(indexInfos);

        auto finishTimeStamp = TimeUtility::currentTimeInSeconds();
        _finishStepString +=
            MergeStep_Name(MS_END_MERGE) + "[" + AdminTaskBase::getDateFormatString(finishTimeStamp) + "];";
        BS_LOG(INFO, "cluster(%s) merge(%s) end", _clusterName.c_str(), _mergeConfigName.c_str());
        return true;
    } else {
        moveToNextStep();
        clearBackupInfo();
        if (!keepWorkerNodeAfterFinish) {
            _nodesStartTimestamp = -1;
            activeNodes.clear();
            _nodeStatusManager->Update(activeNodes);
        } else {
            // TODO may be unused
            for (auto& node : activeNodes) {
                node->setFinished(false);
                node->setSuspended(false);
            }
            _slowNodeDetector.reset();
            _nodeStatusManager->Update(proto::MergerNodes({}));
            generateAndSetTarget(activeNodes);
        }
        return false;
    }
}

bool SingleMergerTask::checkConfigChanged(MergerNodes& activeNodes) const
{
    if (activeNodes.size() == 0) {
        assert(false);
        return true;
    }
    return _configPath != activeNodes[0]->getTargetConfigPath();
}

void SingleMergerTask::fillClusterInfo(SingleClusterInfo* clusterInfo, const MergerNodes& nodes,
                                       const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    clusterInfo->set_partitioncount(_partitionCount);
    clusterInfo->set_clusterstep(MergeStep_Name(_mergeStep));
    MergerInfo* mergerInfo = clusterInfo->mutable_mergerinfo();
    mergerInfo->set_parallelnum(_mergeParallelNum);
    mergerInfo->set_mergeconfigname(_mergeConfigName);
    fillPartitionInfos(clusterInfo->mutable_mergerinfo(), nodes, roleCounterMap);
    mergerInfo->set_taskstatus(getTaskStatusString());
}

bool SingleMergerTask::updateKeepCheckpointCount(ResourceReaderPtr resourceReader)
{
    bool isConfigExist = false;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.keep_checkpoint_count",
                                                      _checkpointKeepCount, isConfigExist)) {
        BS_LOG(ERROR, "parse cluster_config.keep_checkpoint_count failed for cluster[%s]", _clusterName.c_str());
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME,
                      "start merger failed: parse cluster_config.keep_checkpoint_count failed", *_beeperTags);
        return false;
    }
    IndexPartitionOptionsWrapper optionsWrapper;
    if (!optionsWrapper.load(*(resourceReader.get()), _clusterName)) {
        BS_LOG(ERROR, "load IndexPartitionOptions failed for cluster[%s]", _clusterName.c_str());
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "start merger failed: load IndexPartitionOptions failed",
                      *_beeperTags);
        return false;
    }

    indexlib::config::IndexPartitionOptions options;
    if (!optionsWrapper.getIndexPartitionOptions("", options)) {
        BS_LOG(ERROR, "get default IndexPartitionOption failed for cluster[%s]", _clusterName.c_str());
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "start merger failed: get default IndexPartitionOption failed",
                      *_beeperTags);
        return false;
    }
    uint32_t keepVersionCount = options.GetBuildConfig(false).keepVersionCount;
    if (!isConfigExist) {
        _checkpointKeepCount = max(keepVersionCount / 2, 1u);
        BS_LOG(INFO, "keep_checkpoint_count derived from keepVersionCount[%d] for cluster[%s] is [%d]",
               keepVersionCount, _clusterName.c_str(), _checkpointKeepCount);
    } else if (_checkpointKeepCount < keepVersionCount / 2) {
        BS_LOG(INFO,
               "keep_checkpoint_count for cluster[%s] is [%d], less than keepVersionCount [%d] / 2, adjust to [%d]",
               _clusterName.c_str(), _checkpointKeepCount, keepVersionCount, keepVersionCount / 2);
        _checkpointKeepCount = keepVersionCount / 2;
    } else {
        BS_LOG(INFO, "keep_checkpoint_count for cluster[%s] is [%d]", _clusterName.c_str(), _checkpointKeepCount);
    }
    return true;
}

bool SingleMergerTask::updateConfig()
{
    if (_workerProtocolVersion < 2) {
        return true;
    }
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    auto reader = readerAccessor->getConfig(_clusterName, _schemaVersion);
    if (!reader) {
        return false;
    }
    _configPath = reader->getOriginalConfigPath();
    return true;
}

void SingleMergerTask::prepareMerge(int32_t workerPathVersion)
{
    _workerPathVersion = workerPathVersion;
    _mergeParallelNum = getMergeParallelNum(_mergeConfigName);
    _timestamp = TimeUtility::currentTimeInSeconds();
    _finishStepString.clear();
}

void SingleMergerTask::endMerge(::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos)
{
    assert(_indexCkpAccessor);
    if (!_disableFillIndexInfo && indexInfos.Get(0).isvalid()) {
        if (!_indexCkpAccessor->hasFullIndexInfo(_clusterName)) {
            // for fullIndexInfos
            _indexCkpAccessor->updateIndexInfo(true, _clusterName, indexInfos);
        }
        // for indexInfos
        _indexCkpAccessor->updateIndexInfo(false, _clusterName, indexInfos);
    }

    proto::CheckpointInfo checkpoint;
    IndexCheckpointFormatter::convertToCheckpoint(indexInfos.Get(0), checkpoint);
    int64_t indexSize = getTotalRemainIndexSize();
    _indexCkpAccessor->addIndexCheckpoint(_clusterName, _alignedVersionId, _checkpointKeepCount, checkpoint);
    BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                         "merger add checkpoint, versionId[%d], versionTimeStamp[%lu], "
                         "schemaId[%ld], processorCheckpoint[%ld], ongoingOp[%s], totalIndexSize[%lu]",
                         checkpoint.versionid(), checkpoint.versiontimestamp(), checkpoint.schemaid(),
                         checkpoint.processorcheckpoint(), checkpoint.ongoingmodifyopids().c_str(), indexSize);

    CheckpointAccessorPtr ckpAccessor;
    _resourceManager->getResource(ckpAccessor);

    string totalIndexSizeCkp = StringUtil::toString(indexSize);
    string totalIndexSizeCkpId = IndexCheckpointFormatter::getIndexTotalSizeCheckpointId(_clusterName);
    if (!ckpAccessor->updateCheckpoint(totalIndexSizeCkpId, BS_INDEX_TOTAL_SIZE_ID, totalIndexSizeCkp)) {
        ckpAccessor->addCheckpoint(totalIndexSizeCkpId, BS_INDEX_TOTAL_SIZE_ID, totalIndexSizeCkp);
    }
    switchToInc();
}

bool SingleMergerTask::switchToInc()
{
    _optimizeMerge = false;
    _buildParallelNum = 1;
    _mergeStep = MS_BEGIN_MERGE;
    return true;
}

void SingleMergerTask::collectAlignVersion(MergerNodes& activeNodes)
{
    versionid_t alignedVersionId = -1;
    for (MergerNodes::iterator it = activeNodes.begin(); it != activeNodes.end(); ++it) {
        const MergerNodePtr& node = *it;
        const MergerCurrent& current = node->getCurrentStatus();
        alignedVersionId = alignedVersionId > current.targetversionid() ? alignedVersionId : current.targetversionid();
    }
    _alignedVersionId = alignedVersionId;
}

bool SingleMergerTask::enableAlterFieldVersionFromEnv()
{
    string param = autil::EnvUtil::getEnv("enable_alter_field_version");
    bool enable = true;
    if (param.empty() || !StringUtil::fromString(param, enable)) {
        return true;
    }
    return enable;
}

void SingleMergerTask::collectIndexInfo(MergerNodes& activeNodes,
                                        ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos)
{
    bool isValid = true;
    if (_mergeConfigName == BS_ALTER_FIELD_MERGE_CONFIG && !enableAlterFieldVersionFromEnv()) {
        isValid = false;
    }

    BuilderCheckpoint builderCheckpoint;
    IndexCheckpointFormatter::decodeBuilderCheckpoint(_builderCheckpoint, builderCheckpoint);
    auto finishTimeStamp = TimeUtility::currentTimeInSeconds();
    int64_t totalRemainSize = 0;
    for (const auto& node : activeNodes) {
        const proto::PartitionId& pid = node->getPartitionId();
        bool isBackup = pid.has_backupid() && pid.backupid() != 0;
        if (isBackup) {
            continue;
        }
        auto indexInfo = node->getCurrentStatus().indexinfo();
        indexInfo.set_schemaversion(_schemaVersion);
        indexInfo.set_finishtimestamp(finishTimeStamp);
        indexInfo.set_starttimestamp(builderCheckpoint.builderstarttime());
        indexInfo.set_processorcheckpoint(builderCheckpoint.processorcheckpoint());
        indexInfo.set_processordatasourceidx(builderCheckpoint.processordatasourceidx());
        indexInfo.set_isvalid(isValid);
        indexInfo.set_ongoingmodifyopids(_opIds);
        *indexInfos.Add() = indexInfo;
        totalRemainSize += indexInfo.totalremainindexsize();
    }
    _remainIndexSize = totalRemainSize;
}

uint32_t SingleMergerTask::getMergeParallelNum(const string& mergeConfigName) const
{
    config::ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    ResourceReaderPtr resourceReader = configAccessor->getConfig(_configPath);
    string key = "offline_index_config.customized_merge_config." + mergeConfigName + ".merge_parallel_num";
    uint32_t mergeParallelNum = (uint32_t)-1;
    resourceReader->getClusterConfigWithJsonPath(_clusterName, key, mergeParallelNum);
    if (mergeParallelNum == (uint32_t)-1) {
        mergeParallelNum = 1;
        resourceReader->getClusterConfigWithJsonPath(
            _clusterName, "cluster_config.builder_rule_config.merge_parallel_num", mergeParallelNum);
    }
    return mergeParallelNum;
}

void SingleMergerTask::moveToNextStep()
{
    assert(_mergeStep < MS_END_MERGE);
    auto finishTimeStamp = TimeUtility::currentTimeInSeconds();
    _finishStepString += MergeStep_Name(_mergeStep) + "[" + AdminTaskBase::getDateFormatString(finishTimeStamp) + "];";

    MergeStep nextStep = (MergeStep)((int)_mergeStep + 1);
    BS_LOG(INFO, "move cluster(%s) merge step from [%s] to [%s], opIds[%s]", _clusterName.c_str(),
           MergeStep_Name(_mergeStep).c_str(), MergeStep_Name(nextStep).c_str(), _opIds.c_str());
    BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                         "merge step from [%s] to [%s], schemaVersion [%ld]", MergeStep_Name(_mergeStep).c_str(),
                         MergeStep_Name(nextStep).c_str(), _schemaVersion);
    _mergeStep = nextStep;
}

void SingleMergerTask::generateAndSetTarget(MergerNodes& nodes) const
{
    auto target = generateTargetStatus();
    if (_mergeStep != MS_DO_MERGE) {
        for (MergerNodes::iterator it = nodes.begin(); it != nodes.end(); it++) {
            (*it)->setTargetStatus(target);
        }
        return;
    }
    auto suspendTarget = target;
    suspendTarget.set_suspendtask(true);
    for (auto it = nodes.begin(); it != nodes.end(); it++) {
        const proto::PartitionId& pid = (*it)->getPartitionId();
        std::string roleGroupName = proto::RoleNameGenerator::generateRoleGroupName(pid);
        if (_nodeStatusManager->IsFinishedGroup(roleGroupName) && !(*it)->isFinished()) {
            /*
                 suspend the node that belong to the finished roleGroups
                 to prevent unexpected situation
            */
            (*it)->setTargetStatus(suspendTarget);
        } else {
            (*it)->setTargetStatus(target);
        }
    }
}

MergerTarget SingleMergerTask::generateTargetStatus() const
{
    MergerTarget target;
    target.Clear();
    target.set_configpath(_configPath);
    target.set_partitioncount(_partitionCount);
    target.set_buildparallelnum(_buildParallelNum);
    target.set_mergeparallelnum(_mergeParallelNum);
    target.mutable_mergetask()->set_mergeconfigname(_mergeConfigName);
    target.mutable_mergetask()->set_timestamp(_timestamp);
    target.set_mergestep(_mergeStep);
    if (_mergeStep == MS_END_MERGE && _alignVersion) {
        target.set_alignedversionid(_alignedVersionId);
    }
    target.set_optimizemerge(_optimizeMerge);
    target.set_workerpathversion(_workerPathVersion);

    KeyValueMap kvMap;
    if (!_opIds.empty()) {
        kvMap[OPERATION_IDS] = _opIds;
    }
    if (_batchMask != -1) {
        kvMap[BATCH_MASK] = StringUtil::toString(_batchMask);
    }

    string schemaChangeStandard;
    if (_indexCkpAccessor->getSchemaChangedStandard(_clusterName, schemaChangeStandard)) {
        kvMap[SCHEMA_PATCH] = schemaChangeStandard;
    }

    if (!kvMap.empty()) {
        target.set_targetdescription(ToJsonString(kvMap, true));
    }

    set<versionid_t> versions =
        IndexCheckpointAccessor::getReservedVersions(_clusterName, _indexCkpAccessor, _builderCkpAccessor);
    string versionStr = StringUtil::toString(versions.begin(), versions.end());
    target.set_reservedversions(versionStr);
    return target;
}

bool SingleMergerTask::doWaitSuspended(proto::MergerNodes& mergerNodes)
{
    MergerTarget suspendTarget = generateTargetStatus();
    suspendTarget.set_suspendtask(true);
    bool allSuspended = true;
    for (const auto& mergerNode : mergerNodes) {
        if (mergerNode->isSuspended() || mergerNode->isFinished()) {
            continue;
        }
        if (isSuspended(mergerNode)) {
            mergerNode->setSuspended(true);
        } else {
            allSuspended = false;
            mergerNode->setTargetStatus(suspendTarget);
        }
    }
    return allSuspended;
}

proto::BuildStep SingleMergerTask::getStep() const { return _buildStep; }

bool SingleMergerTask::finish(const KeyValueMap& kvMap) { return true; }

void SingleMergerTask::clearFullWorkerZkNode(const std::string& generationDir) const
{
    bool exists = false;
    string checkpointFlagPath = fslib::util::FileUtil::joinFilePath(generationDir, NO_MERGER_CHECKPOINT_FILE_NAME);
    if (!fslib::util::FileUtil::isExist(checkpointFlagPath, exists)) {
        BS_LOG(WARN, "check %s failed", checkpointFlagPath.c_str());
        return;
    }
    if (!exists) {
        // legacy worker, do not clean
        return;
    }
    doClearFullWorkerZkNode(generationDir, ROLE_MERGER, _clusterName);
}

int64_t SingleMergerTask::getTotalRemainIndexSize() const { return _optimizeMerge ? 0 : _remainIndexSize; }

}} // namespace build_service::admin
