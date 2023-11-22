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
#include "build_service/common/IndexCheckpointResourceKeeper.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <set>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/Admin.pb.h"

using namespace std;
using autil::StringUtil;

namespace build_service { namespace common {
BS_LOG_SETUP(common, IndexCheckpointResourceKeeper);

IndexCheckpointResourceKeeper::IndexCheckpointResourceKeeper(const std::string& name, const std::string& type,
                                                             const ResourceContainerPtr& resourceContainer)
    : ResourceKeeper(name, type)
    , _targetVersion(indexlib::INVALID_VERSIONID)
    , _resourceContainer(resourceContainer)
    , _needLatestVersion(false)
{
    updateVersion(_targetVersion);
    if (resourceContainer) {
        CheckpointAccessorPtr checkpointAccessor;
        resourceContainer->getResource(checkpointAccessor);
        if (checkpointAccessor) {
            _checkpointAccessor.reset(new IndexCheckpointAccessor(checkpointAccessor));
        }
    }
}

IndexCheckpointResourceKeeper::~IndexCheckpointResourceKeeper() {}

bool IndexCheckpointResourceKeeper::init(const KeyValueMap& params)
{
    _clusterName = getValueFromKeyValueMap(params, "clusterName");
    _parameters[config::INDEX_CLUSTER] = _clusterName;
    if (_clusterName.empty()) {
        BS_LOG(ERROR, "IndexCheckpointResourceKeeper init failed: name [%s] cluster [%s]", _resourceName.c_str(),
               _clusterName.c_str());
        return false;
    }
    string version = getValueFromKeyValueMap(params, "version");
    if (version == "latest") {
        _needLatestVersion = true;
        updateVersion(indexlib::INVALID_VERSIONID);
    } else {
        if (!StringUtil::fromString(version, _targetVersion)) {
            BS_LOG(ERROR, "[%s] invalid version param: [%s].", _resourceName.c_str(), version.c_str());
            return false;
        }
        updateVersion(_targetVersion);
    }
    if (!_resourceContainer) {
        BS_LOG(ERROR, "failed to get container");
        return false;
    }
    if (!getIndexRootFromConfig()) {
        return false;
    }
    return true;
}

bool IndexCheckpointResourceKeeper::getIndexRootFromConfig()
{
    config::ConfigReaderAccessorPtr configReaderAccessor;
    _resourceContainer->getResource(configReaderAccessor);
    if (!configReaderAccessor) {
        BS_LOG(ERROR, "failed to get config reader accessor");
        return false;
    }
    config::ResourceReaderPtr configReader = configReaderAccessor->getLatestConfig();
    if (!configReader) {
        BS_LOG(ERROR, "failed to get config reader");
        return false;
    }
    config::BuildServiceConfig buildServiceConfig;
    if (!configReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        string errorMsg = "failed to get build_app.json in config[" + configReader->getConfigPath() + "]";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    string indexRoot = buildServiceConfig.getIndexRoot();
    _parameters[config::RESOURCE_INDEX_ROOT] = indexRoot;
    return true;
}

void IndexCheckpointResourceKeeper::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    ResourceKeeper::Jsonize(json);
    json.Jsonize("cluster_name", _clusterName);
    json.Jsonize("need_latest_version", _needLatestVersion);
    json.Jsonize("target_version", _targetVersion);
}

void IndexCheckpointResourceKeeper::updateVersion(versionid_t version)
{
    _targetVersion = version;
    _parameters[config::INDEX_VERSION] = StringUtil::toString(_targetVersion);
}

void IndexCheckpointResourceKeeper::savepointLatestVersion()
{
    proto::CheckpointInfo checkpointInfo;
    std::string checkpointName;
    if (!_checkpointAccessor->getLatestIndexCheckpoint(_clusterName, checkpointInfo, checkpointName)) {
        // allow no checkpoint
        return;
    }
    versionid_t targetVersion = checkpointInfo.versionid();
    for (auto& applyer : _applyers) {
        string role = applyer + "_" + _resourceName;
        if (!_checkpointAccessor->createIndexSavepoint(role, _clusterName, targetVersion)) {
            assert(false);
            return;
        }
    }
    updateVersion(targetVersion);
}

bool IndexCheckpointResourceKeeper::prepareResource(const std::string& applyer,
                                                    const config::ResourceReaderPtr& configReader)
{
    if (!_checkpointAccessor) {
        BS_LOG(ERROR, "no checkpoint accessor");
        return false;
    }
    _applyers.push_back(applyer);
    if (_needLatestVersion) {
        savepointLatestVersion();
        return true;
    }

    string role = applyer + "_" + _resourceName;
    if (!_checkpointAccessor->createIndexSavepoint(role, _clusterName, _targetVersion)) {
        BS_LOG(ERROR, "cluster [%s] savepoint version [%d] failed.", _clusterName.c_str(), _targetVersion);
        return false;
    }
    return true;
}

string IndexCheckpointResourceKeeper::getResourceId() { return StringUtil::toString(_targetVersion); }

void IndexCheckpointResourceKeeper::deleteApplyer(const std::string& applyer)
{
    if (!_checkpointAccessor) {
        return;
    }
    string role = applyer + "_" + _resourceName;
    auto versions = _checkpointAccessor->getReservedVersions(_clusterName);
    auto iter = versions.begin();
    for (; iter != versions.end(); iter++) {
        auto versionid = *iter;
        if (_checkpointAccessor->isSavepoint(_clusterName, versionid)) {
            _checkpointAccessor->removeIndexSavepoint(role, _clusterName, versionid);
        }
    }
    vector<string> tmpApplyers;
    tmpApplyers.swap(_applyers);
    for (auto& tmpApplyer : tmpApplyers) {
        if (tmpApplyer == applyer) {
            continue;
        }
        _applyers.push_back(tmpApplyer);
    }
}

void IndexCheckpointResourceKeeper::clearUselessCheckpoints(const std::string& applyer,
                                                            const proto::WorkerNodes& workerNodes)
{
    versionid_t smallestVersionid = indexlib::INVALID_VERSIONID;
    for (size_t i = 0; i < workerNodes.size(); i++) {
        proto::WorkerNodeBase::ResourceInfo* resourceInfo;
        if (!workerNodes[i]->getUsingResource(_resourceName, resourceInfo)) {
            return;
        }
        versionid_t version = indexlib::INVALID_VERSIONID;
        if (!StringUtil::fromString(resourceInfo->resourceId, version)) {
            return;
        }
        if (i == 0) {
            smallestVersionid = version;
        } else {
            if (smallestVersionid > version) {
                smallestVersionid = version;
            }
        }
    }
    string role = applyer + "_" + _resourceName;
    auto versions = _checkpointAccessor->getReservedVersions(_clusterName);
    auto iter = versions.begin();
    for (; iter != versions.end(); iter++) {
        auto versionid = *iter;
        if (versionid < smallestVersionid) {
            _checkpointAccessor->removeIndexSavepoint(role, _clusterName, versionid);
        }
    }
}

void IndexCheckpointResourceKeeper::syncResources(const std::string& applyer, const proto::WorkerNodes& workerNodes)
{
    clearUselessCheckpoints(applyer, workerNodes);
    if (_needLatestVersion) {
        savepointLatestVersion();
    }
    string resourceStr;
    for (auto node : workerNodes) {
        proto::WorkerNodeBase::ResourceInfo* resourceInfo = NULL;
        if (!node->getTargetResource(_resourceName, resourceInfo)) {
            if (resourceStr.empty()) {
                resourceStr = ToJsonString(*this);
            }
            node->addTargetDependResource(_resourceName, resourceStr, getResourceId());
        } else {
            versionid_t version = indexlib::INVALID_VERSIONID;
            StringUtil::fromString(resourceInfo->resourceId, version);
            if (version != _targetVersion) {
                if (resourceStr.empty()) {
                    resourceStr = ToJsonString(*this);
                }
                node->addTargetDependResource(_resourceName, resourceStr, getResourceId());
            }
        }
    }
}

bool IndexCheckpointResourceKeeper::updateResource(const KeyValueMap& params)
{
    auto iter = params.find("visiable");
    if (iter != params.end()) {
        auto value = iter->second;
        bool isTrue = false;
        if (!StringUtil::parseTrueFalse(value, isTrue)) {
            BS_LOG(ERROR, "parse true of false failed st r[%s]", value.c_str());
            return false;
        }
        _checkpointAccessor->setIndexVisiable(_clusterName, isTrue);
    }
    return true;
}

}} // namespace build_service::common
