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
#include "build_service/admin/taskcontroller/AlterFieldTask.h"

#include <assert.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/CheckpointTools.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/TaskIdentifier.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;

using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointFormatter;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, AlterFieldTask);

AlterFieldTask::~AlterFieldTask() {}

bool AlterFieldTask::init(proto::BuildStep buildStep)
{
    if (buildStep != proto::BUILD_STEP_INC) {
        BS_LOG(ERROR, "AlterField only execute in inc step");
        return false;
    }
    SingleMergerTask::init(buildStep, "alter_field");
    return true;
}

bool AlterFieldTask::start(const KeyValueMap& kvMap)
{
    if (!SingleMergerTask::start(kvMap)) {
        return false;
    }

    _needWaitAlterField = false;
    auto iter = kvMap.find("targetOperationId");
    if (iter == kvMap.end()) {
        BS_LOG(ERROR, "no target operation id for alter field");
        return false;
    }
    int64_t opId = 0;
    if (!StringUtil::fromString(iter->second, opId)) {
        return false;
    }
    _opsId = iter->second;
    updateLatestVersionToAlterField();
    return true;
}

string AlterFieldTask::getTaskIdentifier() const
{
    proto::TaskIdentifier taskId;
    taskId.setTaskId(_opsId);
    return taskId.toString();
}

void AlterFieldTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    SingleMergerTask::Jsonize(json);
    json.Jsonize("current_checkpoint", _currentCheckpoint, _currentCheckpoint);
    json.Jsonize("next_checkpoint", _nextCheckpoint, _nextCheckpoint);
    json.Jsonize("finish_checkpoint", _finishCheckpoint, _finishCheckpoint);
    json.Jsonize("operation_id", _opsId, _opsId);
    json.Jsonize("can_finish", _canFinish, _canFinish);
}

std::string AlterFieldTask::getAlterFieldCheckpointId(const std::string& clusterName)
{
    return clusterName + "_alter_field";
}

std::string AlterFieldTask::getRoleName() { return _clusterName + "_alter_field_" + _opsId; }

proto::MergerTarget AlterFieldTask::generateTargetStatus() const
{
    auto target = SingleMergerTask::generateTargetStatus();
    KeyValueMap kvMap;
    kvMap["targetVersion"] = StringUtil::toString(_currentCheckpoint.versionId);
    kvMap["operationId"] = _opsId;
    kvMap["checkpointVersion"] = StringUtil::toString(_finishCheckpoint.versionId);
    target.set_targetdescription(ToJsonString(kvMap));
    return target;
}

void AlterFieldTask::endMerge(::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos) { switchToInc(); }

void AlterFieldTask::createSavepoint(const TargetCheckpoint& checkpoint)
{
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    if (checkpoint.versionId == -1) {
        assert(false);
        return;
    }
    string applyRole = getRoleName();
    string errMsg;
    bool ret =
        checkpointAccessor->createSavepoint(applyRole, checkpoint.checkpointId, checkpoint.checkpointName, errMsg);
    if (!ret) {
        BS_LOG(ERROR, "createSavepoint failed, errorMsg[%s]", errMsg.c_str());
    }
}

bool AlterFieldTask::finish(const KeyValueMap& kvMap)
{
    _canFinish = true;
    return true;
}

void AlterFieldTask::removeSavepoint(const TargetCheckpoint& checkpoint)
{
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    if (checkpoint.versionId == -1) {
        return;
    }
    string applyRole = getRoleName();
    string errMsg;
    bool ret =
        checkpointAccessor->removeSavepoint(applyRole, checkpoint.checkpointId, checkpoint.checkpointName, errMsg);
    if (!ret) {
        BS_LOG(ERROR, "removeSavepoint failed, errorMsg[%s]", errMsg.c_str());
    }
}

void AlterFieldTask::updateCheckpoint()
{
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    checkpointAccessor->addCheckpoint(getAlterFieldCheckpointId(_clusterName), _opsId, "");
}

bool AlterFieldTask::getOpsIdFromCheckpointName(const std::string& checkpointName, int64_t& opsId)
{
    return StringUtil::fromString(checkpointName, opsId);
}

bool AlterFieldTask::run(WorkerNodes& workerNodes)
{
    updateLatestVersionToAlterField();
    if (_currentCheckpoint.versionId == -1) {
        if (_canFinish) {
            updateCheckpoint();
            int64_t endTs = autil::TimeUtility::currentTimeInMicroSeconds();
            _taskStatus = TASK_FINISHED;
            removeSavepoint(_finishCheckpoint);
            BS_LOG(INFO, "[%s] alter field [%s] finished, beginTs[%ld] endTs[%ld] duration[%ld]", _clusterName.c_str(),
                   _opsId.c_str(), _nodesStartTimestamp, endTs, (endTs - _nodesStartTimestamp) / 1000000);
            BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                                 "[%s] alter field [%s] finished, beginTs[%ld] endTs[%ld] duration[%ld]",
                                 _clusterName.c_str(), _opsId.c_str(), _nodesStartTimestamp, endTs,
                                 (endTs - _nodesStartTimestamp) / 1000000);
            return true;
        }
        return false;
    }

    if (SingleMergerTask::run(workerNodes) || (_mergeStep != proto::MergeStep::MS_END_MERGE && isOpIdDisable())) {
        removeSavepoint(_finishCheckpoint);
        _finishCheckpoint = _currentCheckpoint;
        _currentCheckpoint = _nextCheckpoint;
        _nextCheckpoint = TargetCheckpoint();
        _taskStatus = TASK_NORMAL;
    }
    return false;
}

bool AlterFieldTask::isOpIdDisable() const
{
    CheckpointAccessorPtr ckpAccessor;
    _resourceManager->getResource(ckpAccessor);
    vector<schema_opid_t> disableOpIds;
    if (!CheckpointTools::getDisableOpIds(ckpAccessor, _clusterName, disableOpIds)) {
        return false;
    }
    int64_t currentOpId = -1;
    if (!StringUtil::fromString(_opsId, currentOpId)) {
        return false;
    }
    for (schema_opid_t opId : disableOpIds) {
        if (currentOpId == opId) {
            BS_LOG(INFO, "opId[%ld] is disable, stop AlterFieldTask for [%s]", currentOpId, _clusterName.c_str());
            return true;
        }
    }
    return false;
}

void AlterFieldTask::updateLatestVersionToAlterField()
{
    // builder and merger max version id
    // schemaId is smaller than current schemaId
    TargetCheckpoint latestCheckpoint;
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);

    // merger checkpoint
    string checkpointId = IndexCheckpointFormatter::getIndexCheckpointId(true, _clusterName);
    string checkpointName;
    string checkpointValue;
    checkpointAccessor->getLatestCheckpoint(checkpointId, checkpointName, checkpointValue);
    proto::CheckpointInfo mergerCheckpoint;
    IndexCheckpointFormatter::decodeCheckpoint(checkpointValue, mergerCheckpoint);

    if (mergerCheckpoint.schemaid() < _schemaVersion) {
        latestCheckpoint.versionId = mergerCheckpoint.versionid();
        latestCheckpoint.checkpointName = checkpointName;
        latestCheckpoint.checkpointId = checkpointId;
    }

    // builder checkpoint
    string builderCheckpointId = IndexCheckpointFormatter::getBuilderCheckpointId(_clusterName);
    checkpointAccessor->getLatestCheckpoint(builderCheckpointId, checkpointName, checkpointValue);
    proto::BuilderCheckpoint builderCheckpoint;
    IndexCheckpointFormatter::decodeBuilderCheckpoint(checkpointValue, builderCheckpoint);
    if (builderCheckpoint.schemaversion() < _schemaVersion) {
        if (builderCheckpoint.versionid() > latestCheckpoint.versionId) {
            latestCheckpoint.versionId = builderCheckpoint.versionid();
            latestCheckpoint.checkpointName = checkpointName;
            latestCheckpoint.checkpointId = builderCheckpointId;
        }
    }

    if (_finishCheckpoint.versionId >= latestCheckpoint.versionId) {
        return;
    }

    if (_currentCheckpoint.versionId >= latestCheckpoint.versionId ||
        _nextCheckpoint.versionId >= latestCheckpoint.versionId) {
        return;
    }

    createSavepoint(latestCheckpoint);
    removeSavepoint(_nextCheckpoint);

    if (_currentCheckpoint.versionId == -1) {
        _currentCheckpoint = latestCheckpoint;
    } else {
        _nextCheckpoint = latestCheckpoint;
    }
    return;
}

string AlterFieldTask::getTaskPhaseIdentifier() const
{
    return string("alter_field_phase_") + MergeStep_Name(_mergeStep);
}

void AlterFieldTask::doSupplementLableInfo(KeyValueMap& info) const
{
    info["merge_step"] = MergeStep_Name(_mergeStep);
    info["merge_config_name"] = _mergeConfigName;
    info["schema_version"] = StringUtil::toString(_schemaVersion);
    info["prepare_merge_timestamp"] = AdminTaskBase::getDateFormatString(_timestamp);
    info["finish_steps"] = _finishStepString;
    info["modify_opid"] = _opsId;
    info["can_finish"] = _canFinish ? "true" : "false";
    info["current_target_version"] = StringUtil::toString(_currentCheckpoint.versionId);
    info["last_finish_version"] = StringUtil::toString(_finishCheckpoint.versionId);
    info["next_target_version"] = StringUtil::toString(_nextCheckpoint.versionId);
}

}} // namespace build_service::admin
