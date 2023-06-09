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
#include "navi/engine/Biz.h"
#include "navi/engine/BizManager.h"
#include "autil/legacy/jsonizable.h"
#include "navi/engine/Kernel.h"
#include "navi/ops/ResourceKernelDef.h"
#include "navi/proto/GraphDef.pb.h"
#include "kmonitor/client/MetricsReporter.h"
#include "navi/resource/KmonResource.h"

using namespace kmonitor;

namespace navi {

Biz::Biz(BizManager *bizManager,
         const std::string &bizName,
         const CreatorManager &creatorManager)
    : _bizManager(bizManager)
    , _bizName(bizName)
    , _creatorManager(creatorManager)
    , _partCount(INVALID_NAVI_PART_COUNT)
{
}

Biz::~Biz() {
    for (const auto &pair : _graphInfoMap) {
        delete pair.second;
    }
    for (const auto &pair : _resourceManagers) {
        delete pair.second;
    }
}

bool Biz::preInit(const std::string &configPath,
                  const NaviBizConfig &config,
                  const Biz *oldBiz)
{
    initConfigPath(configPath, config);
    if (!initConfig(config)) {
        return false;
    }
    if (!createResourceManager(config, oldBiz)) {
        return false;
    }
    if (!loadGraphs(config)) {
        return false;
    }
    return true;
}

bool Biz::saveInitResource(NaviPartId partCount,
                           NaviPartId partId,
                           const ResourceMap &resourceMap)
{
    if (_partCount != partCount) {
        NAVI_KERNEL_LOG(ERROR, "partCount mismatch, expect [%d], got [%d]",
                        _partCount, partCount);
        return false;
    }
    auto resourceManager = getResourceManager(partId);
    if (!resourceManager) {
        NAVI_KERNEL_LOG(ERROR, "partId [%d] not exist", partId);
        return false;
    }
    if (!resourceManager->addResource(resourceMap)) {
        return false;
    }
    NAVI_KERNEL_LOG(INFO,
                    "biz [%s] partCount [%d] partId [%d] init resource [%s]",
                    _bizName.c_str(), partCount, partId,
                    autil::StringUtil::toString(resourceMap).c_str());
    return true;
}

bool Biz::postInit(const Biz &defaultBiz) {
    auto defaultResourceManager = defaultBiz.getResourceManager(0);
    assert(defaultResourceManager);
    for (const auto &pair : _resourceManagers) {
        if (!pair.second->postInit(defaultResourceManager)) {
            return false;
        }
    }
    return true;
}

void Biz::addResourceGraph(const std::string &resource,
                           const MultiLayerResourceMap &multiLayerResourceMap,
                           GraphBuilder &builder) const
{
    return _bizManager->addResourceGraph(this, resource, multiLayerResourceMap,
                                         builder);
}

bool Biz::createResourceManager(const NaviBizConfig &config, const Biz *oldBiz) {
    const auto &partIds = config.partIds;
    if (partIds.empty()) {
        NAVI_KERNEL_LOG(ERROR, "no partId in biz [%s] config [%s]",
                        _bizName.c_str(), FastToJsonString(config).c_str());
        return false;
    }
    auto partCount = config.partCount;
    for (auto partId : config.partIds) {
        if (partId >= partCount) {
            NAVI_KERNEL_LOG(ERROR, "invalid partId: [%d], not less than "
                                   "partCount: [%d], bizName: [%s]",
                            partId, partCount, _bizName.c_str());
            return false;
        }
        ResourceManager *oldResourceManager = nullptr;
        if (oldBiz) {
            oldResourceManager = oldBiz->getResourceManager(partId);
        }
        auto manager =
            new ResourceManager(this, _bizName, _configPath, partCount, partId,
                                _creatorManager, _resourceConfigMap);
        if (!manager->preInit(oldResourceManager)) {
            delete manager;
            return false;
        }
        _resourceManagers.emplace(partId, manager);
    }
    _partCount = partCount;
    return true;
}

ResourceManager *Biz::getResourceManager(NaviPartId partId) const {
    auto it = _resourceManagers.find(partId);
    if (_resourceManagers.end() != it) {
        return it->second;
    }
    return nullptr;
}

bool Biz::loadGraphs(const NaviBizConfig &config) {
    for (const auto &pair : config.graphConfigMap) {
        auto graphInfo = new GraphInfo(_configPath, pair.second);
        if (graphInfo->init()) {
            _graphInfoMap[pair.first] = graphInfo;
        } else {
            delete graphInfo;
            return false;
        }
    }
    return true;
}

const GraphInfo *Biz::getGraphInfo(const std::string &name) const {
    auto it = _graphInfoMap.find(name);
    if (_graphInfoMap.end() == it) {
        return nullptr;
    } else {
        return it->second;
    }
}

void Biz::collectResourceMap(NaviPartId partCount,
                             NaviPartId partId,
                             MultiLayerResourceMap &multiLayerResourceMap) const
{
    auto resourceManager = getResourceManager(partId);
    if (!resourceManager) {
        return;
    }
    resourceManager->collectResourceMap(multiLayerResourceMap);
}

ResourcePtr Biz::createResource(NaviPartId partCount,
                                NaviPartId partId,
                                const std::string &name,
                                const MultiLayerResourceMap &multiLayerResourceMap)
{
    auto resourceManager = getResourceManager(partId);
    if (!resourceManager) {
        NAVI_KERNEL_LOG(ERROR,
                        "create resource [%s] failed, partId [%d] not exist",
                        name.c_str(), partId);
        return nullptr;
    }
    auto expectPartCount = resourceManager->getPartCount();
    if (partCount != expectPartCount) {
        NAVI_KERNEL_LOG(
            ERROR,
            "partCount mismatch, expect: [%d], actual: [%d], bizName: %s",
            expectPartCount, partCount, _bizName.c_str());
        return nullptr;
    }
    return resourceManager->createResource(name, multiLayerResourceMap);
}

const KernelCreator *Biz::getKernelCreator(const std::string &name) const {
    return _creatorManager.getKernelCreator(name);
}

const ResourceCreator *Biz::getResourceCreator(const std::string &name) const {
    return _creatorManager.getResourceCreator(name);
}

void Biz::initConfigPath(const std::string &configPath,
                         const NaviBizConfig &config)
{
    if (!config.configPath.empty()) {
        _configPath = config.configPath;
    } else {
        _configPath = configPath;
    }
}

bool Biz::initConfig(const NaviBizConfig &config) {
    if (!_kernelConfigMap.init(config.kernelConfigVec)) {
        return false;
    }
    if (!_resourceConfigMap.init(config.resourceConfigVec)) {
        return false;
    }
    auto &kernelConfigMap = _kernelConfigMap.getConfigMap();
    for (auto &pair : kernelConfigMap) {
        const auto &name = pair.first;
        auto &registryConfig = pair.second;
        auto &dependResourceMap = registryConfig.dependResourceMap;
        if (dependResourceMap.empty()) {
            continue;
        }
        auto creator = getKernelCreator(name);
        if (!creator) {
            NAVI_KERNEL_LOG(ERROR, "kernel [%s] has config but not registered",
                            name.c_str());
            return false;
        }
        const auto &dependMapFromCreator = creator->getDependResources();
        dependResourceMap.insert(dependMapFromCreator.begin(),
                                 dependMapFromCreator.end());
    }
    auto &resourceConfigMap = _resourceConfigMap.getConfigMap();
    for (auto &pair : resourceConfigMap) {
        const auto &name = pair.first;
        auto &registryConfig = pair.second;
        auto &dependResourceMap = registryConfig.dependResourceMap;
        if (dependResourceMap.empty()) {
            continue;
        }
        auto creator = getResourceCreator(name);
        if (!creator) {
            NAVI_KERNEL_LOG(ERROR, "resource [%s] has config but not registered",
                            name.c_str());
            return false;
        }
        const auto &dependMapFromCreator = creator->getDependResources();
        dependResourceMap.insert(dependMapFromCreator.begin(),
                                 dependMapFromCreator.end());
    }
    return true;
}

autil::legacy::RapidValue *Biz::getKernelConfig(const std::string &name) const {
    return _kernelConfigMap.getConfig(name);
}

const std::map<std::string, bool> *Biz::getKernelDependResourceMap(
        const std::string &name) const
{
    auto registryConfig = _kernelConfigMap.getRegistryConfig(name);
    if (registryConfig && !registryConfig->dependResourceMap.empty()) {
        return &registryConfig->dependResourceMap;
    }
    auto creator = getKernelCreator(name);
    if (!creator) {
        return nullptr;
    }
    return &creator->getDependResources();
}

const std::map<std::string, bool> *Biz::getResourceDependResourceMap(
        const std::string &name) const
{
    auto registryConfig = _resourceConfigMap.getRegistryConfig(name);
    if (registryConfig && !registryConfig->dependResourceMap.empty()) {
        return &registryConfig->dependResourceMap;
    }
    auto creator = getResourceCreator(name);
    if (!creator) {
        return nullptr;
    }
    return &creator->getDependResources();
}

const std::string &Biz::getConfigPath() const {
    return _configPath;
}

const std::string &Biz::getName() const {
    return _bizName;
}

bool Biz::isSinglePart() const {
    return _partCount == 1;
}

bool Biz::hasPartId(NaviPartId partId) const {
    return _resourceManagers.end() != _resourceManagers.find(partId);
}

}
