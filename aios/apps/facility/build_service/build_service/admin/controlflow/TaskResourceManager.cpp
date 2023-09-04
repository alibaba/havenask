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
#include "build_service/admin/controlflow/TaskResourceManager.h"

#include "build_service/common/ResourceKeeperCreator.h"
#include "build_service/common/SwiftResourceKeeper.h"

using namespace std;

using autil::legacy::Any;
using autil::legacy::Jsonizable;
using build_service::common::ResourceContainerPtr;
using build_service::common::ResourceKeeperCreator;
using build_service::common::ResourceKeeperGuard;
using build_service::common::ResourceKeeperGuardPtr;
using build_service::common::ResourceKeeperPtr;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskResourceManager);

TaskResourceManager::TaskResourceManager() : _resourceContainer(new common::ResourceContainer), _version(0) {}

TaskResourceManager::~TaskResourceManager() {}

bool TaskResourceManager::declareResourceKeeper(const std::string& type, const KeyValueMap& params)
{
    autil::ScopedLock lock(_lock);
    string resourceName = getValueFromKeyValueMap(params, "name");
    if (resourceName.empty()) {
        BS_LOG(ERROR, "resourceName is empty");
        return false;
    }
    if (_resourceKeeperMap.find(resourceName) != _resourceKeeperMap.end()) {
        BS_LOG(WARN, "resourceKeeper[%s] is already exist!", resourceName.c_str());
        return true;
    }

    ResourceKeeperPtr resourceKeeper = ResourceKeeperCreator::create(resourceName, type, _resourceContainer);
    if (!resourceKeeper) {
        return false;
    }
    if (!resourceKeeper->init(params)) {
        BS_LOG(ERROR, "resourceKeeper name [%s] type[%s] init fail!", resourceName.c_str(), type.c_str());
        return false;
    }
    _resourceKeeperMap[resourceName] = resourceKeeper;
    return true;
}

ResourceKeeperPtr TaskResourceManager::getResourceKeeper(const KeyValueMap& params)
{
    autil::ScopedLock lock(_lock);
    string resourceName = getValueFromKeyValueMap(params, "name");
    if (resourceName.empty()) {
        BS_LOG(ERROR, "resourceName is empty");
        return nullptr;
    }
    auto iter = _resourceKeeperMap.find(resourceName);
    if (iter == _resourceKeeperMap.end()) {
        BS_LOG(ERROR, "resource [%s] is not exist", resourceName.c_str());
        return nullptr;
    }
    return iter->second;
}

ResourceKeeperInfo::ResourceKeeperInfo(const common::ResourceContainerPtr& resourceContainer)
    : resourceContainer(resourceContainer)
{
}

void ResourceKeeperInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("resource_type", type);
    json.Jsonize("resource_name", name);
    if (json.GetMode() == FROM_JSON) {
        resourceKeeper = ResourceKeeperCreator::create(name, type, resourceContainer);
    }
    json.Jsonize("resource_keeper", *resourceKeeper);
}

std::string TaskResourceManager::serializeResourceKeepers()
{
    vector<ResourceKeeperInfo> keepers;
    keepers.reserve(_resourceKeeperMap.size());
    for (auto iter = _resourceKeeperMap.begin(); iter != _resourceKeeperMap.end(); iter++) {
        ResourceKeeperInfo info(_resourceContainer);
        info.type = iter->second->getType();
        info.name = iter->second->getResourceName();
        info.resourceKeeper = iter->second;
        keepers.push_back(info);
    }
    return ToJsonString(keepers);
}

bool TaskResourceManager::deserializeResourceKeepers(const std::string& jsonStr)
{
    if (jsonStr.empty()) {
        return true;
    }
    try {
        vector<Any> values;
        FromJsonString(values, jsonStr);
        for (auto& value : values) {
            Jsonizable::JsonWrapper jsonWrapper(value);
            ResourceKeeperInfo resourceKeeperInfo(_resourceContainer);
            resourceKeeperInfo.Jsonize(jsonWrapper);
            _resourceKeeperMap[resourceKeeperInfo.name] = resourceKeeperInfo.resourceKeeper;
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "from string fail: [%s] is invalid.", jsonStr.c_str());
        return false;
    }
    return true;
}

common::ResourceKeeperGuardPtr TaskResourceManager::applyResource(const std::string& roleName,
                                                                  const std::string& resourceName,
                                                                  const config::ResourceReaderPtr& configReader)
{
    auto iter = _resourceKeeperMap.find(resourceName);
    if (iter == _resourceKeeperMap.end()) {
        BS_LOG(ERROR, "no resource name [%s], [%s] apply fail", resourceName.c_str(), roleName.c_str());
        return common::ResourceKeeperGuardPtr();
    }
    ResourceKeeperGuardPtr keeper(new ResourceKeeperGuard);
    stringstream applyerName;
    applyerName << roleName << "_" << _version;
    if (!keeper->init(applyerName.str(), configReader, iter->second)) {
        return common::ResourceKeeperGuardPtr();
    }
    _version++;
    return keeper;
}

}} // namespace build_service::admin
