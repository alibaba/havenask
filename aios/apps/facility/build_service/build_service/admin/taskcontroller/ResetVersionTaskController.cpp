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
#include "build_service/admin/taskcontroller/ResetVersionTaskController.h"

#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ProtoUtil.h"
#include "fslib/util/FileUtil.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ResetVersionTaskController);

ResetVersionTaskController::ResetVersionTaskController(const string& taskId, const string& taskName,
                                                       const TaskResourceManagerPtr& resMgr)
    : DefaultTaskController(taskId, taskName, resMgr)
{
}

ResetVersionTaskController::~ResetVersionTaskController() {}

bool ResetVersionTaskController::doInit(const string& clusterName, const string& taskConfigFilePath,
                                        const string& initParam)
{
    _isFinished = false;
    _parallelNum = 1;
    _partitionCount = 1;
    map<string, string> taskParam;
    try {
        autil::legacy::FromJsonString(taskParam, initParam);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "INVALID json str[%s]", initParam.c_str());
        return false;
    }
    config::TaskTarget target(DEFAULT_TARGET_NAME);
    target.setPartitionCount(_partitionCount);
    target.setParallelNum(_parallelNum);

    auto getParam = [taskParam](const std::string& key, std::string& value) -> bool {
        auto iter = taskParam.find(key);
        if (iter == taskParam.end()) {
            BS_LOG(ERROR, "lack of [%s] for reset version task", key.c_str());
            return false;
        }
        value = iter->second;
        return true;
    };
    if ((!getParam("buildId", _buildId)) || (!getParam("versionId", _versionId))) {
        return false;
    }

    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    config::ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    if (!validateCluster(resourceReader)) {
        return false;
    }
    if (!readIndexRoot(resourceReader, _indexRoot)) {
        BS_LOG(ERROR, "readIndexRoot failed");
        return false;
    }
    if (!readPartitionCount(resourceReader, _indexPartitionCount)) {
        BS_LOG(ERROR, "readPartitionCount failed");
        return false;
    }

    target.addTargetDescription("buildId", _buildId);
    target.addTargetDescription("clusterName", _clusterName);
    target.addTargetDescription("indexRoot", _indexRoot);
    target.addTargetDescription("versionId", _versionId);
    target.addTargetDescription("partitionCount", autil::StringUtil::toString(_indexPartitionCount));

    _targets.clear();
    _targets.push_back(target);
    return true;
}

bool ResetVersionTaskController::validateCluster(const config::ResourceReaderPtr& resourceReader) const
{
    vector<string> clusterNames;
    proto::BuildId buildId;
    if (!proto::ProtoUtil::strToBuildId(_buildId, buildId)) {
        BS_LOG(ERROR, "fail to parse buildId from [%s]", _buildId.c_str());
        return false;
    }
    if (!resourceReader->getAllClusters(buildId.datatable(), clusterNames)) {
        string errorMsg = "getAllClusters for [" + buildId.datatable() + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (const auto& cluster : clusterNames) {
        if (cluster == _clusterName) {
            return true;
        }
    }
    BS_LOG(ERROR, "do not have cluster [%s] in buildId [%s]", _clusterName.c_str(), _buildId.c_str());
    return false;
}
bool ResetVersionTaskController::readIndexRoot(const config::ResourceReaderPtr& resourceReader, string& indexRoot) const
{
    config::BuildServiceConfig buildServiceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        string errorMsg = "failed to get build_app.json in config[" + resourceReader->getConfigPath() + "]";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    indexRoot = buildServiceConfig.getIndexRoot();

    string indexPathPrefix;
    if (autil::EnvUtil::getEnvWithoutDefault("BUILD_SERVICE_TEST_INDEX_PATH_PREFIX", indexPathPrefix)) {
        size_t pos = indexRoot.find("://");
        if (pos != string::npos) {
            indexRoot = fslib::util::FileUtil::joinFilePath(indexPathPrefix, indexRoot.substr(pos + 3));
        }
    }
    return true;
}

bool ResetVersionTaskController::readPartitionCount(const config::ResourceReaderPtr& resourceReader,
                                                    uint32_t& indexPartitionCount) const
{
    config::BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        std::string errorMsg =
            string("parse cluster_config.builder_rule_config for [") + _clusterName + string("] failed");
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    indexPartitionCount = buildRuleConfig.partitionCount;
    return true;
}
bool ResetVersionTaskController::start(const KeyValueMap& kvMap)
{
    if (_isFinished) {
        BS_LOG(ERROR, "ResetVersionTaskController start() is not re-accessible");
        return false;
    }
    return true;
}

bool ResetVersionTaskController::updateConfig()
{
    BS_LOG(ERROR, "do not support update config in reset version task");
    return false;
}
bool ResetVersionTaskController::operate(TaskController::Nodes& taskNodes)
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
    if (taskNodes[0].reachedTarget) {
        taskNodes.clear();
        _isFinished = true;
        BS_LOG(INFO, "finish reset version task buildId : [%s], cluster [%s]", _buildId.c_str(), _clusterName.c_str());
        return true;
    }
    return false;
}

void ResetVersionTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    DefaultTaskController::Jsonize(json);
    json.Jsonize("is_finished", _isFinished, _isFinished);
    json.Jsonize("build_id", _buildId, _buildId);
    json.Jsonize("index_root", _indexRoot, _indexRoot);
    json.Jsonize("version_id", _versionId, _versionId);
    json.Jsonize("index_partition_count", _indexPartitionCount, _indexPartitionCount);
}

}} // namespace build_service::admin
