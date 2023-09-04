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
#include "build_service/admin/taskcontroller/SyncIndexTaskController.h"

#include "build_service/admin/CheckpointCreator.h"
#include "build_service/common/CheckpointResourceKeeper.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigReaderAccessor.h"

using namespace std;
using namespace autil::legacy;
using namespace build_service::config;
using autil::StringUtil;

namespace build_service { namespace admin {
BS_LOG_SETUP(taskcontroller, SyncIndexTaskController);

SyncIndexTaskController::SyncIndexTaskController(const std::string& taskId, const std::string& taskName,
                                                 const TaskResourceManagerPtr& resMgr)
    : DefaultTaskController(taskId, taskName, resMgr)
    , _totalTargetIdx(-1)
    , _keepVersionCount(1)
    , _targetVersion(INVALID_VERSION)
    , _needNewTarget(true)
    , _preferSyncIndex(true)
{
    _indexCkpAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
}

SyncIndexTaskController::~SyncIndexTaskController() {}

bool SyncIndexTaskController::doInit(const string& clusterName, const string& taskConfigFilePath,
                                     const string& initParam)
{
    _partitionCount = 1;
    _parallelNum = 1;
    map<string, string> taskParam;
    try {
        FromJsonString(taskParam, initParam);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "INVALID json str[%s]", initParam.c_str());
        return false;
    }

    SyncIndexTaskConfig syncConfig;
    if (!getTaskConfigValue(syncConfig)) {
        BS_LOG(ERROR, "sync index config is invalid");
        return false;
    }
    _madroxAddr = syncConfig.madroxAddr;
    _remoteIndexRoot = syncConfig.remoteIndexPath;
    _keepVersionCount = syncConfig.keepVersionCount;
    _preferSyncIndex = syncConfig.preferSyncIndex;

    _syncResourceName = getValueFromKeyValueMap(taskParam, "resourceName");
    _cluster = clusterName;
    KeyValueMap kvMap;
    kvMap["name"] = _syncResourceName;
    auto resourceKeeper = _resourceManager->getResourceKeeper(kvMap);
    if (!resourceKeeper) {
        BS_LOG(ERROR, "failed to get resource keeper, resource name [%s]", _syncResourceName.c_str());
        return false;
    }
    _keeper = DYNAMIC_POINTER_CAST(common::CheckpointResourceKeeper, resourceKeeper);
    if (!_keeper.get()) {
        BS_LOG(ERROR, "failed to get checkpoint resource keeper, resource name [%s]", _syncResourceName.c_str());
        return false;
    }
    return true;
}

bool SyncIndexTaskController::getTaskConfigValue(SyncIndexTaskConfig& config)
{
    ConfigReaderAccessorPtr configReaderAccessor;
    _resourceManager->getResource(configReaderAccessor);
    auto configReader = configReaderAccessor->getLatestConfig();
    std::string configPath = configReader->getOriginalConfigPath();
    if (!configReader) {
        BS_LOG(ERROR, "get sync index lastest config reader failed");
        return false;
    }
    bool isExist = false;
    configReader->getTaskConfigWithJsonPath(configPath, _cluster, "sync_index", "", config, isExist);
    if (!isExist) {
        BS_LOG(ERROR, "lock of sync index config");
        return false;
    }

    if (config.madroxAddr.empty()) {
        BS_LOG(ERROR, "lock of [%s]", MADROX_ADMIN_ADDRESS.c_str());
        return false;
    }

    if (config.remoteIndexPath.empty()) {
        BS_LOG(ERROR, "lock of [%s]", BS_SYNC_INDEX_ROOT.c_str());
        return false;
    }

    if (config.keepVersionCount <= 0) {
        BS_LOG(ERROR, "invalid keep version count [%lu]", config.keepVersionCount);
        return false;
    }
    BS_LOG(INFO, "sync index keep version count [%lu]", config.keepVersionCount);

    BS_LOG(INFO, "prefer sync index is [%d]", config.preferSyncIndex);

    if (_originalIndexRoot.empty()) {
        BuildServiceConfig serviceConfig;
        if (!configReader->getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
            BS_LOG(ERROR, "cannot get build app");
            return false;
        }
        _originalIndexRoot = serviceConfig.getIndexRoot();
    }
    return true;
}

bool SyncIndexTaskController::createNewTarget(
    config::TaskTarget* target, const common::CheckpointResourceKeeper::CheckpointResourceInfoVec& currentResourceInfo)
{
    proto::CheckpointInfo checkpointInfo;
    std::string checkpointName;
    if (!_indexCkpAccessor->getLatestIndexCheckpoint(_cluster, checkpointInfo, checkpointName)) {
        BS_LOG(WARN, "failed to get latest index checkpoint");
        return false;
    }
    versionid_t latestVersion = checkpointInfo.versionid();
    TaskTarget tmpTarget;
    tmpTarget.setPartitionCount(_partitionCount);
    tmpTarget.setParallelNum(_parallelNum);
    tmpTarget.addTargetDescription("clusterName", _cluster);
    tmpTarget.addTargetDescription(BS_SYNC_INDEX_ROOT, _remoteIndexRoot);
    tmpTarget.addTargetDescription(MADROX_ADMIN_ADDRESS, _madroxAddr);
    set<versionid_t> versionsSet;
    vector<versionid_t> versions;
    for (const auto& info : currentResourceInfo) {
        if (info.status == common::CheckpointResourceKeeper::CRS_SAVEPOINTED) {
            versionid_t version = INVALID_VERSION;
            if (StringUtil::fromString(getValueFromKeyValueMap(info.param, INDEX_VERSION), version) &&
                version != INVALID_VERSION) {
                versionsSet.insert(version);
            }
        }
    }
    versions.reserve(versionsSet.size() + 1);
    for (auto version : versionsSet) {
        versions.push_back(version);
    }

    if ((!versions.empty() && latestVersion != *versions.rbegin()) || versions.empty()) {
        versions.push_back(latestVersion);
    }
    tmpTarget.addTargetDescription("versions", ToJsonString(versions));
    if (!_targets.empty() && _targets.rbegin()->getTargetDescription() == tmpTarget.getTargetDescription()) {
        BS_LOG(DEBUG, "new target is same as old, no need to create");
        return false;
    }
    *target = tmpTarget;
    _targetVersion = latestVersion;
    if (!updateIndexSavepoint(versions)) {
        BS_LOG(ERROR, "update savepoint for cluster [%s], version [%s] failed", _cluster.c_str(),
               ToJsonString(versions).c_str());
        return false;
    }
    return true;
}

bool SyncIndexTaskController::updateIndexSavepoint(const vector<versionid_t>& reservedVersions)
{
    BS_LOG(INFO, "update index savepoint [%s]", ToJsonString(reservedVersions).c_str());
    string role = "sync_index_" + _cluster;
    set<versionid_t> versions = _indexCkpAccessor->getReservedVersions(_cluster);
    for (auto it : versions) {
        if (find(reservedVersions.begin(), reservedVersions.end(), it) == reservedVersions.end()) {
            _indexCkpAccessor->removeIndexSavepoint(role, _cluster, it);
        } else {
            if (!_indexCkpAccessor->createIndexSavepoint(role, _cluster, it)) {
                BS_LOG(ERROR, "create savepoint for cluster [%s], version [%d] failed", _cluster.c_str(), it);
                return false;
            }
        }
    }
    return true;
}

bool SyncIndexTaskController::updateConfig()
{
    string madroxAddr, remoteIndexPath;
    SyncIndexTaskConfig syncConfig;
    if (!getTaskConfigValue(syncConfig)) {
        BS_LOG(ERROR, "sync index config is invalid");
        return false;
    }
    if (syncConfig.madroxAddr != _madroxAddr || syncConfig.remoteIndexPath != _remoteIndexRoot) {
        _needNewTarget = true;
    }
    _madroxAddr = syncConfig.madroxAddr;
    _remoteIndexRoot = syncConfig.remoteIndexPath;
    _keepVersionCount = syncConfig.keepVersionCount;
    _preferSyncIndex = syncConfig.preferSyncIndex;

    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    _configPath = resourceReader->getOriginalConfigPath();
    return true;
}

void SyncIndexTaskController::tryUpdateTarget(
    const common::CheckpointResourceKeeper::CheckpointResourceInfoVec& currentResourceInfo, TaskController::Node& node)
{
    TaskTarget target, lastTarget;
    if (!createNewTarget(&target, currentResourceInfo)) {
        return;
    }

    _needNewTarget = false;
    BS_LOG(INFO, "switch to processing, target [%s]", ToJsonString(target).c_str());
    _currentTargetIdx = _targets.size();
    _totalTargetIdx++;
    target.setTargetName(StringUtil::toString(_totalTargetIdx));
    _targets.push_back(target);
    if (_targets.size() > TARGET_KEEP_COUNT) {
        _targets.erase(_targets.begin(), _targets.begin() + (_targets.size() - TARGET_KEEP_COUNT));
    }
    node.taskTarget = *(_targets.rbegin());
}

bool SyncIndexTaskController::operate(TaskController::Nodes& taskNodes)
{
    common::CheckpointResourceKeeper::CheckpointResourceInfoVec currentResourcesInfo = _keeper->getResourceStatus();

    map<versionid_t, versionid_t> usingIndex2resourceMap;
    map<versionid_t, versionid_t> allResourceMap;
    for (const auto& it : currentResourcesInfo) {
        string versionStr = getValueFromKeyValueMap(it.param, INDEX_VERSION);
        versionid_t indexVersion = INVALID_VERSION;
        if (!versionStr.empty()) {
            if (!StringUtil::fromString(versionStr, indexVersion)) {
                continue;
            }
        }
        if (indexVersion == INVALID_VERSION) {
            continue;
        }
        allResourceMap[it.version] = indexVersion;
        if (it.status == common::CheckpointResourceKeeper::CRS_SAVEPOINTED) {
            usingIndex2resourceMap[indexVersion] = it.version;
        }
    }

    if (taskNodes.size() != 1) {
        taskNodes.clear();
    }

    if (taskNodes.size() == 0) {
        TaskController::Node node;
        node.nodeId = 0;
        node.instanceIdx = 0;
        taskNodes.push_back(node);
        tryUpdateTarget(currentResourcesInfo, taskNodes[0]);
        taskNodes[0].taskTarget = *(_targets.rbegin());
        return false;
    }
    KeyValueMap statusMap;
    const auto& statusDesc = taskNodes[0].statusDescription;
    try {
        if (!statusDesc.empty()) {
            FromJsonString(statusMap, statusDesc);
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "INVALID json str[%s]", statusDesc.c_str());
        return false;
    }
    if (_needNewTarget) {
        tryUpdateTarget(currentResourcesInfo, taskNodes[0]);
    } else if (taskNodes[0].reachedTarget) {
        string value = getValueFromKeyValueMap(statusMap, "versions");
        BS_LOG(INFO, "reach target, synced version is [%s]", value.c_str());

        if (!value.empty()) {
            vector<versionid_t> syncedVersions;
            FromJsonString(syncedVersions, value);
            if (!syncedVersions.empty()) {
                BS_LOG(INFO, "set hasindex to true");
                SetProperty("hasIndex", "true");
            }
            versionid_t maxSyncedVersion =
                syncedVersions.empty() ? INVALID_VERSION : syncedVersions[syncedVersions.size() - 1];
            versionid_t maxAvailableVersion =
                usingIndex2resourceMap.empty() ? INVALID_VERSION : usingIndex2resourceMap.rbegin()->second;
            if (maxSyncedVersion > maxAvailableVersion) {
                BS_LOG(INFO, "add latest index version [%d]", maxSyncedVersion);
                KeyValueMap kvMap;
                kvMap[INDEX_VERSION] = StringUtil::toString(maxSyncedVersion);
                kvMap[INDEX_CLUSTER] = _cluster;
                string indexRoot = getValueFromKeyValueMap(statusMap, BS_SYNC_INDEX_ROOT);
                if (_preferSyncIndex) {
                    kvMap[RESOURCE_INDEX_ROOT] = indexRoot;
                    kvMap[RESOURCE_BACKUP_INDEX_ROOT] = _originalIndexRoot;
                } else {
                    kvMap[RESOURCE_INDEX_ROOT] = _originalIndexRoot;
                    kvMap[RESOURCE_BACKUP_INDEX_ROOT] = indexRoot;
                }
                kvMap[common::CheckpointResourceKeeper::CHECKPOINT_TYPE] = SYNC_INDEX;
                _keeper->addResource(kvMap);
            }

            auto iter = allResourceMap.begin();
            for (size_t i = 0; i + _keepVersionCount < allResourceMap.size(); ++i) {
                if (usingIndex2resourceMap.find(iter->second) != usingIndex2resourceMap.end()) {
                    iter++;
                    continue;
                }
                KeyValueMap param;
                param[common::CheckpointResourceKeeper::CHECKPOINT_TYPE] = SYNC_INDEX;
                param[common::CheckpointResourceKeeper::RESOURCE_VERSION] = StringUtil::toString(iter->first);
                _keeper->deleteResource(param);
                iter++;
            }
        }
        _needNewTarget = true;
        taskNodes.clear();
        BS_LOG(INFO, "is processing false");
        return false;
    } else if (getValueFromKeyValueMap(statusMap, "need_update_target") == KV_TRUE) {
        tryUpdateTarget(currentResourcesInfo, taskNodes[0]);
        return false;
    }
    return false;
}

void SyncIndexTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    DefaultTaskController::Jsonize(json);
    json.Jsonize("clusters", _cluster, _cluster);
    json.Jsonize("sync_resource_name", _syncResourceName, _syncResourceName);
    json.Jsonize("total_target_idx", _totalTargetIdx, _totalTargetIdx);
    json.Jsonize("keep_version_count", _keepVersionCount, _keepVersionCount);
    json.Jsonize("remote_index_root", _remoteIndexRoot, _remoteIndexRoot);
    json.Jsonize("original_index_root", _originalIndexRoot, _originalIndexRoot);
    json.Jsonize("madrox_addr", _madroxAddr, _madroxAddr);
    json.Jsonize("target_version", _targetVersion, _targetVersion);
    json.Jsonize("need_new_target", _needNewTarget, _needNewTarget);
    json.Jsonize("prefer_sync_index", _preferSyncIndex, _preferSyncIndex);
    if (json.GetMode() == FROM_JSON) {
        KeyValueMap kvMap;
        kvMap["name"] = _syncResourceName;
        auto resourceKeeper = _resourceManager->getResourceKeeper(kvMap);
        _keeper = DYNAMIC_POINTER_CAST(common::CheckpointResourceKeeper, resourceKeeper);
    }
}

}} // namespace build_service::admin
