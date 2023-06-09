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
#include "build_service/common/CheckpointResourceKeeper.h"

#include "autil/StringUtil.h"
#include "build_service/common/ResourceCheckpointFormatter.h"
#include "build_service/config/CLIOptionNames.h"

using namespace std;
using namespace build_service::config;
using namespace autil::legacy;
using autil::StringUtil;

namespace build_service { namespace common {
BS_LOG_SETUP(common, CheckpointResourceKeeper);

CheckpointResourceKeeper::CheckpointResourceKeeper(const std::string& name, const std::string& type,
                                                   const ResourceContainerPtr& resourceContainer)
    : ResourceKeeper(name, type)
    , _resourceVersion(INVALID_VERSION)
    , _needLatestVersion(false)
{
    common::CheckpointAccessorPtr accessor;
    if (resourceContainer) {
        if (resourceContainer->getResource(accessor)) {
            _ckptAccessor = accessor;
            assert(_ckptAccessor.get());
        }
    }
}

CheckpointResourceKeeper::~CheckpointResourceKeeper() {}

bool CheckpointResourceKeeper::init(const KeyValueMap& params)
{
    // type
    auto iter = params.find(CHECKPOINT_TYPE);
    if (iter == params.end()) {
        BS_LOG(ERROR, "[%s] is required, but params is [%s]", CHECKPOINT_TYPE, ToJsonString(params).c_str());
        return false;
    } else {
        _checkpointType = iter->second;
    }

    // need latest version
    iter = params.find(NEED_LATEST_VERSION);
    if (iter != params.end()) {
        if (!StringUtil::fromString(iter->second, _needLatestVersion)) {
            BS_LOG(ERROR, "parse need latest version failed, [%s]", iter->second.c_str());
            return false;
        }
    }

    // version
    string versionStr = getValueFromKeyValueMap(params, RESOURCE_VERSION);
    versionid_t version = INVALID_VERSION;
    if (!versionStr.empty()) {
        if (!StringUtil::fromString(versionStr, version)) {
            BS_LOG(ERROR, "invalid version str[%s]", versionStr.c_str());
            return false;
        }
        _resourceVersion = version;
    }
    return true;
}

bool CheckpointResourceKeeper::updateSelf(const string& applyer, versionid_t newResourceVersion)
{
    if (!_ckptAccessor) {
        BS_LOG(ERROR, "no checkpoint accessor");
        return false;
    }
    if (_needLatestVersion) {
        string ckptName, ckptValue;
        if (!_ckptAccessor->getLatestCheckpoint(getCkptId(_checkpointType), ckptName, ckptValue)) {
            BS_LOG(ERROR, "fail to get latest checkpoint");
            return false;
        }
        newResourceVersion = ResourceCheckpointFormatter::decodeResourceCheckpointName(ckptName);
    }
    if (newResourceVersion != INVALID_VERSION) {
        _resourceVersion = newResourceVersion;
        string ckptId = getCkptId(_checkpointType);
        string ckptName = getCkptName(_resourceVersion);
        string errMsg;
        if (!_ckptAccessor->createSavepoint(applyer, ckptId, ckptName, errMsg)) {
            BS_LOG(ERROR, "failed to create savepoint [%s]", errMsg.c_str());
            return false;
        }
        string ckptValue;
        if (!_ckptAccessor->getCheckpoint(ckptId, ckptName, ckptValue)) {
            BS_LOG(ERROR, "failed to get checkpoint id [%s], name [%s] ", ckptId.c_str(), ckptName.c_str());
            return false;
        }
        KeyValueMap kvMap;
        try {
            if (!ckptValue.empty()) {
                FromJsonString(kvMap, ckptValue);
                _parameters = kvMap;
            }
        } catch (const ExceptionBase& e) {
            string errorMsg = "Invalid  param[" + ckptValue + "] from checkpoint";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;
}

void CheckpointResourceKeeper::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    ResourceKeeper::Jsonize(json);
    json.Jsonize("resource_version", _resourceVersion);
    json.Jsonize("checkpoint_type", _checkpointType);
    json.Jsonize("need_latest_version", _needLatestVersion);
}

bool CheckpointResourceKeeper::prepareResource(const std::string& applyer,
                                               const config::ResourceReaderPtr& configReader)
{
    return updateSelf(applyer, _resourceVersion);
}

void CheckpointResourceKeeper::deleteApplyer(const std::string& applyer)
{
    if (!_ckptAccessor) {
        return;
    }
    string ckptId = getCkptId(_checkpointType);
    vector<pair<string, string>> checkpoints;
    _ckptAccessor->listCheckpoints(ckptId, checkpoints);
    std::string comment;
    for (const pair<string, string>& ckpt : checkpoints) {
        if (_ckptAccessor->isSavepoint(ckptId, ckpt.first, &comment)) {
            string msg;
            _ckptAccessor->removeSavepoint(applyer, ckptId, ckpt.first, msg);
        }
    }
}

string CheckpointResourceKeeper::getResourceId() { return StringUtil::toString(_resourceVersion); }
string CheckpointResourceKeeper::getCkptName(versionid_t resourceVersion) const
{
    return ResourceCheckpointFormatter::encodeResourceCheckpointName(resourceVersion);
}
string CheckpointResourceKeeper::getCkptId(const string& checkpointType) const
{
    return ResourceCheckpointFormatter::getResourceCheckpointId(checkpointType);
}

bool CheckpointResourceKeeper::addResource(const KeyValueMap& params)
{
    if (!_ckptAccessor) {
        BS_LOG(ERROR, "no checkpoint accessor");
        return false;
    }
    versionid_t targetVersion = INVALID_VERSION;
    string ckptName, ckptValue;
    string ckptId;
    auto iter = params.find(CHECKPOINT_TYPE);
    if (iter == params.end()) {
        BS_LOG(ERROR, "[%s] is required, but params is [%s]", CHECKPOINT_TYPE, ToJsonString(params).c_str());
        return false;
    } else {
        ckptId = getCkptId(iter->second);
    }
    if (!_ckptAccessor->getLatestCheckpoint(ckptId, ckptName, ckptValue)) {
        targetVersion = 0;
    } else {
        targetVersion = ResourceCheckpointFormatter::decodeResourceCheckpointName(ckptName) + 1;
    }
    if (ckptValue == ToJsonString(params)) {
        BS_LOG(INFO, "add checkpoint resource [%s], params [%s] already exist", iter->second.c_str(),
               ToJsonString(params).c_str());
        return true;
    }
    _ckptAccessor->addCheckpoint(ckptId, getCkptName(targetVersion), ToJsonString(params));
    BS_LOG(INFO, "add checkpoint resource [%s],version [%d],  params [%s]", iter->second.c_str(), targetVersion,
           ToJsonString(params).c_str());
    return true;
}

bool CheckpointResourceKeeper::deleteResource(const KeyValueMap& params)
{
    if (!_ckptAccessor) {
        BS_LOG(ERROR, "no checkpoint accessor");
        return false;
    }
    versionid_t targetVersion = INVALID_VERSION;
    string ckptName, ckptId;
    auto iter = params.find(CHECKPOINT_TYPE);
    if (iter == params.end()) {
        BS_LOG(ERROR, "[%s] is required, but params is [%s]", CHECKPOINT_TYPE, ToJsonString(params).c_str());
        return false;
    } else {
        ckptId = getCkptId(iter->second);
    }
    iter = params.find(RESOURCE_VERSION);
    if (iter == params.end() || !StringUtil::fromString(iter->second, targetVersion)) {
        BS_LOG(ERROR, "resource version is invalid in param [%s]", ToJsonString(params).c_str());
        return false;
    }
    ckptName = getCkptName(targetVersion);
    _ckptAccessor->cleanCheckpoint(ckptId, ckptName);
    BS_LOG(INFO, "remove checkpoint id is [%s], ckpName is [%s]", ckptId.c_str(), ckptName.c_str());
    return true;
}

void CheckpointResourceKeeper::clearUselessCheckpoints(const std::string& applyer,
                                                       const proto::WorkerNodes& workerNodes)
{
    if (!_ckptAccessor) {
        return;
    }
    versionid_t smallestVersionid = INVALID_VERSION;
    for (size_t i = 0; i < workerNodes.size(); i++) {
        proto::WorkerNodeBase::ResourceInfo* resourceInfo;
        if (!workerNodes[i]->getUsingResource(_resourceName, resourceInfo)) {
            return;
        }
        versionid_t version = INVALID_VERSION;
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
    string ckptId = getCkptId(_checkpointType);
    vector<pair<string, string>> checkpoints;
    _ckptAccessor->listCheckpoints(ckptId, checkpoints);
    for (const pair<string, string>& ckpt : checkpoints) {
        versionid_t version = ResourceCheckpointFormatter::decodeResourceCheckpointName(ckpt.first);
        if (version < smallestVersionid) {
            string msg;
            _ckptAccessor->removeSavepoint(applyer, ckptId, ckpt.first, msg);
        }
    }
}

void CheckpointResourceKeeper::syncResources(const std::string& applyer, const proto::WorkerNodes& workerNodes)
{
    clearUselessCheckpoints(applyer, workerNodes);
    if (_needLatestVersion) {
        updateSelf(applyer, INVALID_VERSION);
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
            if (resourceInfo->resourceId != getResourceId()) {
                if (resourceStr.empty()) {
                    resourceStr = ToJsonString(*this);
                }
                node->addTargetDependResource(_resourceName, resourceStr, getResourceId());
            }
        }
    }
}

CheckpointResourceKeeper::CheckpointResourceInfoVec CheckpointResourceKeeper::getResourceStatus()
{
    CheckpointResourceInfoVec ret;
    string ckptId = getCkptId(_checkpointType);
    vector<pair<string, string>> checkpoints;
    if (!_ckptAccessor) {
        return ret;
    }
    _ckptAccessor->listCheckpoints(ckptId, checkpoints);
    ret.reserve(checkpoints.size());
    for (const pair<string, string>& ckpt : checkpoints) {
        CheckpointResourceInfo info;
        versionid_t resourceVersion = ResourceCheckpointFormatter::decodeResourceCheckpointName(ckpt.first);
        info.version = resourceVersion;
        try {
            if (!ckpt.second.empty()) {
                FromJsonString(info.param, ckpt.second);
            }
        } catch (const ExceptionBase& e) {
            string errorMsg = "Invalid  param[" + ckpt.second + "] from checkpoint";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            continue;
        }

        info.status = CRS_UNSAVEPOINTED;
        std::string comment;
        if (_ckptAccessor->isSavepoint(ckptId, ckpt.first, &comment)) {
            info.status = CRS_SAVEPOINTED;
        }
        ret.push_back(info);
    }
    return ret;
}
}} // namespace build_service::common
