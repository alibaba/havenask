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
#include "build_service/admin/taskcontroller/EndBuildTaskController.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(taskcontroller, EndBuildTaskController);

EndBuildTaskController::EndBuildTaskController(const string& taskId, const string& taskName,
                                               const TaskResourceManagerPtr& resMgr)
    : DefaultTaskController(taskId, taskName, resMgr)
    , _buildVersion(INVALID_VERSION)
    , _workerPathVersion(-1)
    , _schemaId(config::INVALID_SCHEMAVERSION)
    , _buildParallelNum(1)
    , _batchMask(-1)
    , _needAlignedBuildVersion(true)
{
}

EndBuildTaskController::~EndBuildTaskController() {}

bool EndBuildTaskController::doInit(const string& clusterName, const string& taskConfigFilePath,
                                    const string& initParam)
{
    map<string, string> taskParam;
    try {
        FromJsonString(taskParam, initParam);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "INVALID json str[%s]", initParam.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "init endBuild failed: INVALID json str[%s]", initParam.c_str());
        return false;
    }

    auto iter = taskParam.find(BS_ENDBUILD_PARTITION_COUNT);
    if (iter == taskParam.end()) {
        BS_LOG(ERROR, "lack of partition_count for endBuild");
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "init endBuild failed: lack of partition_count for endBuild",
                      *_beeperTags);
        return false;
    }
    if (!StringUtil::fromString(iter->second, _partitionCount)) {
        BS_LOG(ERROR, "INVALID partitionCount[%s] for EndBuild[%s]", iter->second.c_str(), clusterName.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "INVALID partitionCount[%s] for EndBuild[%s]", iter->second.c_str(), clusterName.c_str());
        return false;
    }

    _parallelNum = 1;
    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    _configPath = resourceReader->getOriginalConfigPath();
    TaskTarget target(DEFAULT_TARGET_NAME);
    target.setPartitionCount(_partitionCount);
    target.setParallelNum(_parallelNum);
    _targets.clear();
    _targets.push_back(target);
    return true;
}

bool EndBuildTaskController::updateConfig()
{
    if (_schemaId == config::INVALID_SCHEMAVERSION) {
        BS_LOG(WARN, "endbuild task has not start, not upc config");
        return true;
    }
    ConfigReaderAccessorPtr configReaderAccessor;
    _resourceManager->getResource(configReaderAccessor);
    ResourceReaderPtr resourceReader = configReaderAccessor->getConfig(_clusterName, _schemaId);
    _configPath = resourceReader->getOriginalConfigPath();
    return true;
}

bool EndBuildTaskController::operate(TaskController::Nodes& taskNodes)
{
    if (_currentTargetIdx == _targets.size()) {
        taskNodes.clear();
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "endBuild task finish", *_beeperTags);
        return true;
    }

    if (taskNodes.size() == 0) {
        uint32_t taskNodeNum = getPartitionCount() * getParallelNum();
        taskNodes.reserve(taskNodeNum);
        for (uint32_t i = 0; i < taskNodeNum; ++i) {
            TaskController::Node node;
            node.nodeId = i;
            node.instanceIdx = i;
            node.taskTarget = _targets[_currentTargetIdx];
            taskNodes.push_back(node);
        }
        return false;
    }
    bool isFinished = true;
    for (uint32_t i = 0; i < taskNodes.size(); ++i) {
        if (!(taskNodes[i].reachedTarget && getBuildVersion(taskNodes[i]))) {
            isFinished = false;
        }
    }
    if (!isFinished) {
        return false;
    }
    taskNodes.clear();
    _currentTargetIdx++;

    if (_currentTargetIdx == _targets.size()) {
        _currentTargetIdx = 0;
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "endBuild task finish", *_beeperTags);
        return true;
    }
    return false;
}

versionid_t EndBuildTaskController::getBuilderVersion() const { return _buildVersion; }

bool EndBuildTaskController::getBuildVersion(TaskController::Node& node)
{
    map<string, string> taskStatus;
    try {
        FromJsonString(taskStatus, node.statusDescription);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "getBuildVersion fail, INVALID json str[%s]", node.statusDescription.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "getBuildVersion fail, INVALID json str[%s]", node.statusDescription.c_str());
        return false;
    }
    auto iter = taskStatus.find(BS_ENDBUILD_VERSION);
    if (iter == taskStatus.end()) {
        BS_LOG(ERROR, "endBuild Task[%s] is reach target, but lack of builderVersion", node.roleName.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "endBuild Task[%s] is reach target, but lack of builderVersion", node.roleName.c_str());
        return false;
    }
    versionid_t targetVersion = INVALID_VERSION;
    if (!StringUtil::fromString(iter->second, targetVersion)) {
        BS_LOG(ERROR, "endbuild failed, invalid builderVersion[%s]", iter->second.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "endbuild failed, invalid builderVersion[%s]", iter->second.c_str());
        return false;
    }
    if (_buildVersion != INVALID_VERSION && _buildVersion != targetVersion) {
        BS_LOG(ERROR, "endBuild Task[%s][%u], builder version[%d] not aligned, other endBuild version[%d]",
               node.roleName.c_str(), node.nodeId, targetVersion, _buildVersion);
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "endBuild Task[%s][%u], builder version[%d] not aligned, "
                             "other endBuild version[%d]",
                             node.roleName.c_str(), node.nodeId, targetVersion, _buildVersion);
        if (_needAlignedBuildVersion) {
            return false;
        }
    }

    assert(targetVersion != INVALID_VERSION);

    if (_needAlignedBuildVersion) {
        _buildVersion = targetVersion;
    } else {
        _buildVersion = (_buildVersion == INVALID_VERSION) ? targetVersion : std::min(_buildVersion, targetVersion);
    }
    return true;
}

bool EndBuildTaskController::start(const KeyValueMap& kvMap)
{
    auto iter = kvMap.find(BS_ENDBUILD_WORKER_PATHVERSION);
    if (iter == kvMap.end()) {
        BS_LOG(ERROR, "lack of worker_pathversion for endBuild");
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "start endBuild failed : lack of worker_pathversion",
                      *_beeperTags);
        return false;
    }
    string workerPathVersion = iter->second;
    if (!StringUtil::fromString(workerPathVersion, _workerPathVersion)) {
        BS_LOG(ERROR, "INVALID workerPathVersion[%s] for EndBuild[%s]", iter->second.c_str(), _clusterName.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start endBuild failed: INVALID workerPathVersion[%s]", iter->second.c_str());
        return false;
    }
    assert(_targets.size() == 1);
    string workerPathVersionOld;
    if (_targets[0].getTargetDescription(BS_ENDBUILD_WORKER_PATHVERSION, workerPathVersionOld) &&
        workerPathVersionOld != workerPathVersion) {
        _targets[0].updateTargetDescription(BS_ENDBUILD_WORKER_PATHVERSION, workerPathVersion);
    } else {
        _targets[0].addTargetDescription(BS_ENDBUILD_WORKER_PATHVERSION, workerPathVersion);
    }

    auto indexCkpAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    string schemaChangeStandard;
    if (indexCkpAccessor && indexCkpAccessor->getSchemaChangedStandard(_clusterName, schemaChangeStandard)) {
        _targets[0].addTargetDescription(SCHEMA_PATCH, schemaChangeStandard);
    }

    iter = kvMap.find("schema_id");
    if (iter == kvMap.end()) {
        BS_LOG(ERROR, "lack of schema id for endBuild");
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "start endBuild failed: lack of schema id for endBuild",
                      *_beeperTags);
        return false;
    }
    if (!StringUtil::fromString(iter->second, _schemaId)) {
        BS_LOG(ERROR, "INVALID schemaId[%s] for EndBuild[%s]", iter->second.c_str(), _clusterName.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start endBuild failed: INVALID schemaId [%s]", iter->second.c_str());
        return false;
    }
    // build parallel num
    iter = kvMap.find(BUILD_PARALLEL_NUM);
    if (iter == kvMap.end()) {
        BS_LOG(ERROR, "lack of build_parallel_num for endBuild");
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "start endBuild failed: lack of build_parallel_num",
                      *_beeperTags);
        return false;
    }
    if (!StringUtil::fromString(iter->second, _buildParallelNum)) {
        BS_LOG(ERROR, "INVALID buildParallelNum[%s] for EndBuild[%s]", iter->second.c_str(), _clusterName.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start endBuild failed: INVALID buildParallelNum[%s]", iter->second.c_str());
        return false;
    }
    uint32_t oldParallelNum;
    if (_targets[0].getTargetDescription(BUILD_PARALLEL_NUM, oldParallelNum) && oldParallelNum != _buildParallelNum) {
        BS_LOG(INFO, "update build parallel num[%u -> %u] for %s", oldParallelNum, _buildParallelNum,
               _clusterName.c_str());
        _targets[0].updateTargetDescription(BUILD_PARALLEL_NUM, StringUtil::toString(_buildParallelNum));
    } else {
        _targets[0].addTargetDescription(BUILD_PARALLEL_NUM, StringUtil::toString(_buildParallelNum));
    }

    iter = kvMap.find(BATCH_MASK);
    if (iter != kvMap.end()) {
        if (!StringUtil::fromString(iter->second, _batchMask)) {
            BS_LOG(INFO, "INVALID batch mask[%s] for EndBuild[%s]", iter->second.c_str(), _clusterName.c_str());
        }
    }
    if (_batchMask != -1) {
        BS_LOG(INFO, "use batch mask[%d] for EndBuild[%s]", _batchMask, _clusterName.c_str());
        _targets[0].updateTargetDescription(BATCH_MASK, StringUtil::toString(_batchMask));
    }

    // operation ids
    _buildVersion = INVALID_VERSION;

    // for ImportBuild: imported index may not have aligned buildVersion, turn this switch off to bypass version
    // alignment check
    if (autil::EnvUtil::getEnv("NEED_ALIGNED_BUILD_VERSION", true) == false) {
        BS_LOG(INFO, "do NOT validate buildVersion is aligned in cluster[%s]", _clusterName.c_str());
        _needAlignedBuildVersion = false;
    }

    iter = kvMap.find(OPERATION_IDS);
    if (iter == kvMap.end() || iter->second.empty()) {
        // clear opids
        _opIds = "";
        _targets[0].removeTargetDescription(OPERATION_IDS);
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start endBuild success, config is[%s], buildParallelNum[%u], "
                             "schemaId[%ld], workerPathVersion[%d]",
                             _configPath.c_str(), _buildParallelNum, _schemaId, _workerPathVersion);
        return true;
    }

    _opIds = iter->second;
    string opIds;
    if (_targets[0].getTargetDescription(OPERATION_IDS, opIds) && opIds != _opIds) {
        BS_LOG(INFO, "update ongoing opIds[%s -> %s] for %s", opIds.c_str(), _opIds.c_str(), _clusterName.c_str());
        _targets[0].updateTargetDescription(OPERATION_IDS, _opIds);
    } else {
        _targets[0].addTargetDescription(OPERATION_IDS, _opIds);
    }

    BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                         "start endBuild success, config is[%s], buildParallelNum[%u], "
                         "schemaId[%ld], workerPathVersion[%d], operationId[%s]",
                         _configPath.c_str(), _buildParallelNum, _schemaId, _workerPathVersion, opIds.c_str());
    return true;
}

void EndBuildTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    DefaultTaskController::Jsonize(json);
    json.Jsonize("build_version", _buildVersion, _buildVersion);
    json.Jsonize(WORKER_PATH_VERSION, _workerPathVersion, _workerPathVersion);
    json.Jsonize(OPERATION_IDS, _opIds, _opIds);
    json.Jsonize("schema_id", _schemaId, _schemaId);
    json.Jsonize(BUILD_PARALLEL_NUM, _buildParallelNum, _buildParallelNum);
    json.Jsonize(BATCH_MASK, _batchMask, _batchMask);
    json.Jsonize("need_aligned_build_version", _needAlignedBuildVersion, true);
}

void EndBuildTaskController::supplementLableInfo(KeyValueMap& info) const
{
    info["end_build_version"] = StringUtil::toString(_buildVersion);
    if (!_opIds.empty()) {
        info["ongoing_operation_ids"] = _opIds;
    }
}

}} // namespace build_service::admin
