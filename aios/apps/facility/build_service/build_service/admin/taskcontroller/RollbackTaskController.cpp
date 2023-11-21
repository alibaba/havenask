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
#include "build_service/admin/taskcontroller/RollbackTaskController.h"

#include <assert.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Admin.pb.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace build_service::config;

using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointAccessorPtr;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, RollbackTaskController);

RollbackTaskController::~RollbackTaskController() {}

ResourceReaderPtr RollbackTaskController::getConfigReader(const string& clusterName,
                                                          const TaskResourceManagerPtr& resourceManager)
{
    config::ConfigReaderAccessorPtr readerAccessor;
    resourceManager->getResource(readerAccessor);
    int64_t maxSchemaId = readerAccessor->getMaxSchemaId(clusterName);
    auto configReader = readerAccessor->getConfig(clusterName, maxSchemaId);
    if (!configReader) {
        BS_LOG(ERROR, "get config reader for cluster [%s] failed.", clusterName.c_str());
        return ResourceReaderPtr();
    }
    return configReader;
}

bool RollbackTaskController::checkRollbackLegal(const string& clusterName, int64_t sourceVersion,
                                                const TaskResourceManagerPtr& resourceManager)
{
    auto configReader = getConfigReader(clusterName, resourceManager);
    auto schema = configReader->getSchema(clusterName);
    if (!schema) {
        BS_LOG(ERROR, "rollback %s to %ld failed, lack schema", clusterName.c_str(), sourceVersion);
        return false;
    }
    if (!schema->HasModifyOperations()) {
        return true;
    }

    IndexCheckpointAccessorPtr checkpointAccessor = CheckpointCreator::createIndexCheckpointAccessor(resourceManager);
    assert(checkpointAccessor);
    proto::CheckpointInfo latestCheckpoint;
    string checkpointName;
    if (!checkpointAccessor->getLatestIndexCheckpoint(clusterName, latestCheckpoint, checkpointName)) {
        BS_LOG(ERROR, "can not find latest checkpoint for %s", clusterName.c_str());
        return false;
    }

    if ((latestCheckpoint.has_ongoingmodifyopids() && latestCheckpoint.ongoingmodifyopids() != "") ||
        schema->GetSchemaVersionId() != latestCheckpoint.schemaid()) {
        BS_LOG(ERROR, "[%s] not support rollback when modify schema with operations", clusterName.c_str());
        return false;
    }
    proto::CheckpointInfo rollbackCheckpoint;
    if (!checkpointAccessor->getIndexCheckpoint(clusterName, sourceVersion, rollbackCheckpoint)) {
        BS_LOG(ERROR, "can not find checkpoint for %s", clusterName.c_str());
        return false;
    }
    if ((rollbackCheckpoint.has_ongoingmodifyopids() && rollbackCheckpoint.ongoingmodifyopids() != "") ||
        rollbackCheckpoint.schemaid() != latestCheckpoint.schemaid()) {
        BS_LOG(ERROR,
               "[%s] with modify operation schema not support "
               "rollback beween two schema version index",
               clusterName.c_str());
        return false;
    }
    return true;
}

bool RollbackTaskController::doInit(const string& clusterName, const string& taskConfigFilePath,
                                    const string& initParam)
{
    _isFinished = false;
    _partitionCount = 1;
    _parallelNum = 1;
    map<string, string> taskParam;
    try {
        FromJsonString(taskParam, initParam);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "INVALID json str[%s]", initParam.c_str());
        return false;
    }
    TaskTarget target(DEFAULT_TARGET_NAME);
    target.setPartitionCount(_partitionCount);
    target.setParallelNum(_parallelNum);

    auto iter = taskParam.find(BS_ROLLBACK_SOURCE_VERSION);
    if (iter == taskParam.end()) {
        BS_LOG(ERROR, "lack of sourceVersion to rollback");
        return false;
    }
    if (!StringUtil::fromString(iter->second, _sourceVersion)) {
        BS_LOG(ERROR, "INVALID rollback sourceVersion[%s]", iter->second.c_str());
        return false;
    }
    if (!checkRollbackLegal(_clusterName, _sourceVersion, _resourceManager)) {
        return false;
    }
    target.addTargetDescription(BS_ROLLBACK_SOURCE_VERSION, iter->second);
    _targets.push_back(target);

    // create savepoint
    IndexCheckpointAccessorPtr checkpointAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    assert(checkpointAccessor);
    return checkpointAccessor->createIndexSavepoint("rollbackIndex", _clusterName, _sourceVersion);
}

bool RollbackTaskController::start(const KeyValueMap& kvMap)
{
    if (_isFinished) {
        BS_LOG(ERROR, "RoolbackTaskController start() is not re-accessible");
        return false;
    }
    return true;
}

bool RollbackTaskController::updateConfig() { return true; }

bool RollbackTaskController::operate(TaskController::Nodes& taskNodes)
{
    if (_isFinished) {
        taskNodes.clear();
        return true;
    }
    if (taskNodes.size() == 0) {
        TaskController::Node node;
        node.nodeId = 0;
        node.instanceIdx = 0;
        node.taskTarget = _targets[0];
        taskNodes.push_back(node);
        return false;
    }
    assert(taskNodes.size() == 1);
    if (taskNodes[0].reachedTarget && addCheckpoint(taskNodes[0])) {
        taskNodes.clear();
        _isFinished = true;
        BS_LOG(INFO, "finish rollback [%s] index", _clusterName.c_str());
        // remove savepoint
        IndexCheckpointAccessorPtr checkpointAccessor =
            CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
        assert(checkpointAccessor);
        return checkpointAccessor->removeIndexSavepoint("rollbackIndex", _clusterName, _sourceVersion);
    }
    return false;
}

bool RollbackTaskController::addCheckpoint(TaskController::Node& node)
{
    map<string, string> taskStatus;
    try {
        FromJsonString(taskStatus, node.statusDescription);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "INVALID json str[%s]", node.statusDescription.c_str());
        return false;
    }
    auto iter = taskStatus.find(BS_ROLLBACK_TARGET_VERSION);
    if (iter == taskStatus.end()) {
        BS_LOG(ERROR, "rollBack Task[%s] is reach target, but lack of targetVersion", node.roleName.c_str());
        return false;
    }
    versionid_t targetVersion = indexlib::INVALID_VERSIONID;
    if (!StringUtil::fromString(iter->second, targetVersion)) {
        BS_LOG(ERROR, "roll back failed, invalid target version[%s]", iter->second.c_str());
        return false;
    }

    IndexCheckpointAccessorPtr checkpointAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    assert(checkpointAccessor);
    proto::CheckpointInfo checkpoint;
    // rollback only support rollback main index
    if (!checkpointAccessor->getIndexCheckpoint(_clusterName, _sourceVersion, checkpoint)) {
        BS_LOG(ERROR, "can not find checkpoint for %s", _clusterName.c_str());
        return false;
    }
    checkpoint.set_versionid(targetVersion);
    checkpointAccessor->addIndexCheckpoint(_clusterName, targetVersion, -1, checkpoint);
    return true;
}

void RollbackTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    DefaultTaskController::Jsonize(json);
    json.Jsonize("is_finished", _isFinished, _isFinished);
    json.Jsonize("source_version", _sourceVersion, _sourceVersion);
}
}} // namespace build_service::admin
