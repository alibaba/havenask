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
#include "navi/engine/ResourceManager.h"
#include "navi/engine/Biz.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/ResourceInitContext.h"
#include "navi/log/NaviLogger.h"

namespace navi {

ResourceManager::ResourceManager(const Biz *biz,
                                 const std::string &bizName,
                                 const std::string &configPath,
                                 NaviPartId partCount,
                                 NaviPartId partId,
                                 const CreatorManager &creatorManager,
                                 const NaviRegistryConfigMap &configMap)
    : _biz(biz)
    , _bizName(bizName)
    , _configPath(configPath)
    , _partCount(partCount)
    , _partId(partId)
    , _creatorManager(creatorManager)
    , _configMap(configMap)
    , _oldResourceManager(nullptr)
{
}

ResourceManager::~ResourceManager() {
}

bool ResourceManager::preInit(ResourceManager *oldResourceManager) {
    _oldResourceManager = oldResourceManager;
    return true;
}

bool ResourceManager::postInit(ResourceManager *defaultResourceManager) {
    _oldResourceManager = nullptr;
    return true;
}

bool ResourceManager::addResource(const ResourceMap &resourceMap) {
    return _resourceMap.addResource(resourceMap);
}

void ResourceManager::collectResourceMap(
    MultiLayerResourceMap &multiLayerResourceMap) const
{
    NAVI_KERNEL_LOG(SCHEDULE1, "collect from resource map [%s]",
                    autil::StringUtil::toString(_resourceMap).c_str());
    multiLayerResourceMap.addResourceMap(&_resourceMap);
}

ResourcePtr ResourceManager::createResource(
    const std::string &name,
    const MultiLayerResourceMap &multiLayerResourceMap)
{
    auto resource = getExistResource(name, multiLayerResourceMap);
    if (resource) {
        NAVI_KERNEL_LOG(TRACE3, "reuse resource [%s] [%p]", name.c_str(),
                        resource.get());
        return resource;
    }
    auto creator = _creatorManager.getResourceCreator(name);
    if (!creator) {
        NAVI_KERNEL_LOG(WARN, "resource [%s] not registered, biz: [%s]",
                        name.c_str(), _bizName.c_str());
        return nullptr;
    }
    ResourceMap dependResourceMap;
    auto dependResourceMapPtr = _biz->getResourceDependResourceMap(name);
    assert(dependResourceMapPtr);
    for (const auto &pair : *dependResourceMapPtr) {
        const auto &dependName = pair.first;
        auto require = pair.second;
        auto dependResource =
            getExistResource(dependName, multiLayerResourceMap);
        if (require && !dependResource) {
            NAVI_KERNEL_LOG(WARN,
                            "create resource [%s] failed, lack required depend "
                            "resource [%s]",
                            name.c_str(), dependName.c_str());
            return nullptr;
        }
        dependResourceMap.addResource(dependResource);
    }
    resource = newAndConfigResource(name, creator, nullptr);
    return initResource(resource, dependResourceMap, creator->getBinderInfos());
}

ResourcePtr ResourceManager::getExistResource(
    const std::string &name,
    const MultiLayerResourceMap &multiLayerResourceMap)
{
    auto resource = multiLayerResourceMap.getResource(name);
    NAVI_KERNEL_LOG(SCHEDULE2, "get resource [%s] from [%s], result [%p]",
                    name.c_str(),
                    autil::StringUtil::toString(multiLayerResourceMap).c_str(),
                    resource.get());
    if (resource) {
        return resource;
    }
    return getResourceFromOld(name);
}

ResourcePtr ResourceManager::getResourceFromOld(const std::string &name) const {
    if (!_oldResourceManager) {
        return nullptr;
    }
    auto resource = _oldResourceManager->_resourceMap.getResource(name);
    if (!resource) {
        return nullptr;
    }
    // can reuse?
    return nullptr;
}

ResourcePtr ResourceManager::newAndConfigResource(
        const std::string &name,
        const ResourceCreator *creator,
        autil::mem_pool::Pool *pool) const
{
    ResourcePtr resource = creator->create(pool);
    assert(resource);
    auto *jsonConfig = _configMap.getConfig(name);
    ResourceConfigContext ctx(_configPath, jsonConfig);
    try {
        if (resource->config(ctx)) {
            return resource;
        } else {
            NAVI_KERNEL_LOG(
                WARN, "config resource [%s] failed, configStr [%s], biz: %s",
                name.c_str(),
                jsonConfig
                    ? autil::legacy::FastToJsonString(*jsonConfig).c_str()
                    : "",
                _bizName.c_str());
            return nullptr;
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_KERNEL_LOG(WARN,
                        "config resource [%s] failed, exception [%s], configStr [%s], biz: %s",
                        name.c_str(),
                        e.ToString().c_str(),
                        jsonConfig ? autil::legacy::FastToJsonString(*jsonConfig).c_str() : "",
                        _bizName.c_str());
        return nullptr;
    }
}

ResourcePtr ResourceManager::initResource(
    const ResourcePtr &resourcePtr,
    const ResourceMap &baseResourceMap,
    const ResourceBindInfos &binderInfos) const
{
    if (!resourcePtr) {
        return resourcePtr;
    }
    auto &resource = *resourcePtr;
    const auto &name = resource.getName();
    if (!bindResource(name, resource, baseResourceMap, binderInfos)) {
        NAVI_KERNEL_LOG(
            WARN, "init resource [%s] failed, bind depend resource failed",
            name.c_str());
        return nullptr;
    }
    ResourceInitContext ctx(_configPath, _bizName, _partCount, _partId, baseResourceMap);
    auto ec = resource.init(ctx);
    if (EC_NONE != ec) {
        NAVI_KERNEL_LOG(WARN, "init resource failed, resource [%s], biz: %s",
                        name.c_str(), _bizName.c_str());
        return nullptr;
    }
    resource.addDepend(baseResourceMap);
    NAVI_KERNEL_LOG(TRACE3, "create resource [%s] success: %p, biz: %s",
                    name.c_str(), resourcePtr.get(), _bizName.c_str());
    return resourcePtr;
}

NaviPartId ResourceManager::getPartCount() const {
    return _partCount;
}

bool ResourceManager::hasConfig(const std::string &name) const {
    return _configMap.hasConfig(name);
}
}
