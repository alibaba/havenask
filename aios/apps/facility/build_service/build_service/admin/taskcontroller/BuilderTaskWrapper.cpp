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
#include "build_service/admin/taskcontroller/BuilderTaskWrapper.h"

#include "autil/StringUtil.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/admin/taskcontroller/EndBuildTaskController.h"
#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/WorkerNode.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace beeper;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::util;

using build_service::common::BuilderCheckpointAccessorPtr;
using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointFormatter;

namespace build_service { namespace admin {

BS_LOG_SETUP(taskcontroller, BuilderTaskWrapper);

BuilderTaskWrapper::BuilderTaskWrapper(const BuildId& buildId, const string& clusterName, const string& taskId,
                                       const TaskResourceManagerPtr& resMgr)
    : AdminTaskBase(buildId, resMgr)
    , _phrase(Phrase::BUILD)
    , _clusterName(clusterName)
    , _buildStep(proto::BuildStep::NO_BUILD_STEP)
    , _endBuildBeginTs(0)
    , _batchMask(-1)
{
    _buildTask.reset(new SingleBuilderTask(buildId, clusterName, taskId, resMgr));
    _endBuildTask.reset(new TaskMaintainer(buildId, taskId, BS_TASK_END_BUILD, resMgr));
}

BuilderTaskWrapper::~BuilderTaskWrapper() {}

bool BuilderTaskWrapper::init(proto::BuildStep buildStep)
{
    _buildStep = buildStep;
    if (buildStep == proto::BuildStep::BUILD_STEP_FULL) {
        // full build not support EndBuild
        return _buildTask->init(buildStep);
    }
    // batchMask only affect in inc
    if (!_buildTask->init(buildStep)) {
        BS_LOG(ERROR, "init SingleBuilderTask for [%s] failed.", _buildId.ShortDebugString().c_str());
        return false;
    }
    return initEndBuildTask();
}

void BuilderTaskWrapper::setBeeperTags(const EventTagsPtr beeperTags)
{
    assert(beeperTags);
    assert(_buildTask);
    assert(_endBuildTask);
    AdminTaskBase::setBeeperTags(beeperTags);
    _buildTask->setBeeperTags(beeperTags);
    _endBuildTask->setBeeperTags(beeperTags);
}

bool BuilderTaskWrapper::initEndBuildTask()
{
    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        string errorMsg = "parse cluster_config.builder_rule_config for [" + _clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    uint32_t partitionCount = buildRuleConfig.partitionCount;
    assert(_buildTask);
    map<string, string> taskParam;
    taskParam.insert(make_pair(BS_ENDBUILD_PARTITION_COUNT, StringUtil::toString(partitionCount)));

    string taskParamStr = ToJsonString(taskParam);
    return _endBuildTask->init(_clusterName, resourceReader->getOriginalConfigPath(), taskParamStr);
}

void BuilderTaskWrapper::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(BATCH_MASK, _batchMask, _batchMask);
    if (json.GetMode() == TO_JSON) {
        json.Jsonize("build_phrase", _phrase);
        json.Jsonize("build_task", *_buildTask);
        if (needEndBuild()) {
            json.Jsonize("end_build_task", *_endBuildTask);
        }
        json.Jsonize("cluster_name", _clusterName);
        json.Jsonize("build_step", _buildStep);
        json.Jsonize("has_create_nodes", _hasCreateNodes, _hasCreateNodes);
    } else {
        json.Jsonize("build_phrase", _phrase, Phrase::UNKNOWN);
        if (_phrase == Phrase::UNKNOWN) {
            _buildTask->Jsonize(json);
            _phrase = Phrase::BUILD;
            _clusterName = _buildTask->getClusterName();
            _buildStep = _buildTask->getBuildStep();
            _hasCreateNodes = _buildTask->hasCreateNodes();
        } else {
            json.Jsonize("build_task", *_buildTask);
            json.Jsonize("build_step", _buildStep);

            json.Jsonize("cluster_name", _clusterName);
            json.Jsonize("has_create_nodes", _hasCreateNodes, _hasCreateNodes);
        }

        // init EndBuildTask
        JsonMap jsonMap = json.GetMap();
        auto iter = jsonMap.find("end_build_task");
        if (iter == jsonMap.end()) {
            if (!initEndBuildTask()) {
                throw autil::legacy::ExceptionBase("recover EndBuildTask failed");
            }
        } else {
            json.Jsonize("end_build_task", *_endBuildTask);
        }
    }
}

void BuilderTaskWrapper::addCheckpoint(proto::BuilderCheckpoint& builderCheckpoint)
{
    if (needEndBuild()) {
        const TaskControllerPtr& controller = _endBuildTask->getController();
        EndBuildTaskControllerPtr endBuildController = DYNAMIC_POINTER_CAST(EndBuildTaskController, controller);
        assert(endBuildController);
        assert(endBuildController->getBuilderVersion() != INVALID_VERSION);
        builderCheckpoint.set_versionid(endBuildController->getBuilderVersion());
    }
    BuilderCheckpointAccessorPtr checkpointAccessor =
        CheckpointCreator::createBuilderCheckpointAccessor(_resourceManager);
    checkpointAccessor->clearCheckpoint(_clusterName);
    checkpointAccessor->addCheckpoint(_clusterName, builderCheckpoint);
    BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                         "builder add checkpoint, versionTs[%lu], processorCkp[%ld], "
                         "schemaVersion[%ld], versionId[%ld]",
                         builderCheckpoint.versiontimestamp(), builderCheckpoint.processorcheckpoint(),
                         builderCheckpoint.schemaversion(), builderCheckpoint.versionid());
}

bool BuilderTaskWrapper::run(WorkerNodes& workerNodes)
{
    assert(_phrase != Phrase::UNKNOWN);
    if (_phrase == Phrase::BUILD) {
        if (!_buildTask->run(workerNodes)) {
            _hasCreateNodes = _buildTask->hasCreateNodes();
            return false;
        }
        _hasCreateNodes = _buildTask->hasCreateNodes();
        proto::BuilderCheckpoint ckp;
        if (!_buildTask->getBuilderCheckpoint(ckp)) {
            _taskStatus = TASK_FINISHED;
            return true;
        }
        if (!needEndBuild()) {
            addCheckpoint(ckp);
            _taskStatus = TASK_FINISHED;
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "build is finished no need to endBuild", *_beeperTags);
            return true;
        }
        KeyValueMap kvMap;
        getEndBuildTaskParam(kvMap);
        _endBuildTask->start(kvMap);
        _endBuildTask->updateConfig();
        _endBuildBeginTs = TimeUtility::currentTimeInSeconds();
        _phrase = Phrase::END_BUILD;
        _hasCreateNodes = false;
        return false;
    }

    assert(_phrase == Phrase::END_BUILD);
    if (_endBuildTask->run(workerNodes)) {
        _hasCreateNodes = _endBuildTask->hasCreateNodes();
        proto::BuilderCheckpoint ckp;
        if (_buildTask->getBuilderCheckpoint(ckp)) {
            addCheckpoint(ckp);
        }
        _phrase = Phrase::BUILD;
        _taskStatus = TASK_FINISHED;
        BS_LOG(INFO, "buildId[%s] cluster [%s] endBuild finished.", _buildId.ShortDebugString().c_str(),
               _clusterName.c_str());
        return true;
    }
    _hasCreateNodes = _endBuildTask->hasCreateNodes();
    return false;
}

bool BuilderTaskWrapper::getTaskRunningTime(int64_t& intervalInMicroSec) const
{
    if (_phrase == Phrase::BUILD) {
        return _buildTask->getTaskRunningTime(intervalInMicroSec);
    }
    if (_phrase == Phrase::END_BUILD) {
        return _endBuildTask->getTaskRunningTime(intervalInMicroSec);
    }
    return AdminTaskBase::getTaskRunningTime(intervalInMicroSec);
}

void BuilderTaskWrapper::getEndBuildTaskParam(KeyValueMap& kvMap)
{
    kvMap.emplace(OPERATION_IDS, _buildTask->getOngoingOpIds());
    kvMap.emplace(BS_ENDBUILD_WORKER_PATHVERSION, StringUtil::toString(_buildTask->getWorkerPathVersion()));
    kvMap.emplace("schema_id", StringUtil::toString(_buildTask->getSchemaId()));
    kvMap.emplace(BUILD_PARALLEL_NUM, StringUtil::toString(_buildTask->getBuildParallelNum()));
    kvMap.emplace(BATCH_MASK, StringUtil::toString(_buildTask->getBatchMask()));
}

bool BuilderTaskWrapper::start(const KeyValueMap& kvMap)
{
    _phrase = Phrase::BUILD;
    bool ret = _buildTask->start(kvMap);
    _taskStatus = _buildTask->getTaskStatus();
    return ret;
}

bool BuilderTaskWrapper::finish(const KeyValueMap& kvMap)
{
    if (_phrase == Phrase::BUILD) {
        return _buildTask->finish(kvMap);
    }
    BS_LOG(ERROR, "buildId[%s] build phrase[%d], not allow to call finish method", _buildId.ShortDebugString().c_str(),
           _phrase);
    return false;
}

void BuilderTaskWrapper::waitSuspended(WorkerNodes& workerNodes)
{
    if (_phrase == Phrase::BUILD) {
        _buildTask->waitSuspended(workerNodes);
        _taskStatus = _buildTask->getTaskStatus();
        return;
    }
    if (_phrase == Phrase::END_BUILD) {
        _endBuildTask->waitSuspended(workerNodes);
        _taskStatus = _endBuildTask->getTaskStatus();
        return;
    }
    BS_LOG(ERROR, "buildId[%s] build phrase[%d] is invalid for waitSuspended", _buildId.ShortDebugString().c_str(),
           _phrase);
}

bool BuilderTaskWrapper::updateConfig()
{
    assert(_phrase != Phrase::UNKNOWN);
    if (!needEndBuild()) {
        return _buildTask->updateConfig();
    }
    return _buildTask->updateConfig() && _endBuildTask->updateConfig();
}

void BuilderTaskWrapper::fillClusterInfo(proto::SingleClusterInfo* clusterInfo, const BuilderNodes& nodes,
                                         const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    _buildTask->fillClusterInfo(clusterInfo, nodes, roleCounterMap);
    BuilderInfo* builderInfo = clusterInfo->mutable_builderinfo();
    builderInfo->set_buildphrase(getBuildPhrase());
}

proto::BuildStep BuilderTaskWrapper::getStep() const { return _buildTask->getStep(); }

void BuilderTaskWrapper::clearFullWorkerZkNode(const std::string& generationDir) const
{
    _buildTask->clearFullWorkerZkNode(generationDir);
}

bool BuilderTaskWrapper::isBatchMode() const { return _buildTask->isBatchMode(); }

void BuilderTaskWrapper::notifyStopped()
{
    _buildTask->notifyStopped();
    _taskStatus = TASK_STOPPED;
}

string BuilderTaskWrapper::getTaskPhaseIdentifier() const { return string("builder_phase_") + getBuildPhrase(); }

void BuilderTaskWrapper::doSupplementLableInfo(KeyValueMap& info) const
{
    _buildTask->doSupplementLableInfo(info);
    info["need_end_build"] = needEndBuild() ? "true" : "false";
    info["build_phrase"] = getBuildPhrase();
    if (needEndBuild() && _endBuildBeginTs != 0) {
        info["end_build_start_timestamp"] = AdminTaskBase::getDateFormatString(_endBuildBeginTs);
        _endBuildTask->doSupplementLableInfo(info);
    }
}

bool BuilderTaskWrapper::needEndBuild() const { return _buildStep == proto::BuildStep::BUILD_STEP_INC; }

string BuilderTaskWrapper::getBuildPhrase() const
{
    switch (_phrase) {
    case Phrase::BUILD:
        return "build";
    case Phrase::END_BUILD:
        return "end_build";
    case Phrase::UNKNOWN:
        return "unknown";
    }
    return "unknown";
}

}} // namespace build_service::admin
