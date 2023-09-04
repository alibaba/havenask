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
#include "navi/engine/NamedData.h"
#include "navi/engine/NaviSnapshotSummary.h"
#include "navi/engine/ResourceInitContext.h"
#include "navi/log/NaviLogger.h"
#include "navi/rpc_server/NaviRpcServerR.h"
#include "navi/util/CommonUtil.h"

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
    , _gigRpcServer(nullptr)
{
}

ResourceManager::~ResourceManager() {
    NAVI_KERNEL_LOG(INFO,
                    "resource manager destructed, biz [%s] success, partCount: %d, partId: %d",
                    _bizName.c_str(),
                    _partCount,
                    _partId);
}

bool ResourceManager::preInit(ResourceManager *oldResourceManager) {
    _oldResourceManager = oldResourceManager;
    return true;
}

bool ResourceManager::postInit() {
    return true;
}

bool ResourceManager::saveResource(const ResourceMap &resourceMap) {
    ResourceMap newResourceMap;
    const auto &map = resourceMap.getMap();
    for (const auto &pair : map) {
        const auto &resource = pair.second;
        auto stage = _creatorManager.getResourceStage(pair.first);
        if (stage != RS_EXTERNAL && stage >= RS_RUN_GRAPH_EXTERNAL) {
            continue;
        }
        newResourceMap.addResource(resource);
    }
    newResourceMap.addResource(_resourceMap);
    _resourceMap.setResource(newResourceMap);
    if (NAVI_BUILDIN_BIZ == _bizName) {
        newResourceMap.addResource(_mergedResourceMap);
        _mergedResourceMap.setResource(newResourceMap);
    }
    NAVI_KERNEL_LOG(INFO, "saved resource [%s]", autil::StringUtil::toString(_resourceMap).c_str());
    return true;
}

bool ResourceManager::collectStaticGraphInfo() {
    auto infoMap = std::make_shared<std::unordered_map<std::string, StaticGraphInfoPtr>>();
    std::unordered_map<std::string, std::string> producerMap;
    auto map = _mergedResourceMap.getMap();
    for (const auto &pair : map) {
        const auto &resourceName = pair.first;
        const auto &resource = pair.second;
        std::vector<StaticGraphInfoPtr> graphInfos;
        if (!resource->getStaticGraphInfo(graphInfos)) {
            continue;
        }
        for (const auto &info : graphInfos) {
            if (!info) {
                continue;
            }
            const auto &name = info->name;
            if (name.empty()) {
                NAVI_KERNEL_LOG(ERROR, "empty static graph name, produced by resource [%s]", resourceName.c_str());
                return false;
            }
            if (infoMap->end() != infoMap->find(name)) {
                std::string previousProducer;
                auto it = producerMap.find(name);
                if (producerMap.end() != it) {
                    previousProducer = it->second;
                }
                NAVI_KERNEL_LOG(ERROR,
                                "graph [%s] produced by two resource, [%s] and [%s]",
                                name.c_str(),
                                previousProducer.c_str(),
                                resourceName.c_str());
                return false;
            }
            (*infoMap)[name] = info;
            producerMap[name] = resourceName;
        }
    }
    autil::ScopedLock lock(_publishLock);
    _staticGraphInfoMap = std::move(infoMap);
    return true;
}

bool ResourceManager::publishResource(ResourceManager *buildinResourceManager,
                                      ResourceManager *bizResourceManager,
                                      const ResourceStageMap &resourceStageMap)
{
    if ((buildinResourceManager == this) || (bizResourceManager == this)) {
        NAVI_KERNEL_LOG(ERROR, "buildin or biz resource manager can't publish");
        return false;
    }
    ResourceMap newResourceMap;
    newResourceMap.addResource(_resourceMap);
    if (buildinResourceManager) {
        newResourceMap.addResource(buildinResourceManager->_resourceMap);
    }
    if (bizResourceManager) {
        newResourceMap.addResource(bizResourceManager->_resourceMap);
    }
    _mergedResourceMap.setResource(newResourceMap);
    if (!validateResource(resourceStageMap)) {
        NAVI_KERNEL_LOG(ERROR, "validate resource failed");
        return false;
    }
    if (NAVI_BUILDIN_BIZ != _bizName && NAVI_BIZ_PART_ID != _partId) {
        if (!collectStaticGraphInfo()) {
            return false;
        }
    }
    if (!updatePublishMeta()) {
        return false;
    }
    return true;
}

bool ResourceManager::updatePublishMeta() {
    {
        autil::ScopedLock lock(_publishLock);
        if (!_gigRpcServer) {
            NAVI_KERNEL_LOG(INFO, "publish ignored, wait for publish");
            return true;
        }
    }
    std::vector<multi_call::ServerBizTopoInfo> infoVec;
    if (!collectPublish(infoVec)) {
        return false;
    }
    std::vector<multi_call::SignatureTy> signatureVec;
    if (!_gigRpcServer->publish(infoVec, signatureVec)) {
        NAVI_KERNEL_LOG(ERROR, "publish topo info failed");
        return false;
    }
    return true;
}

bool ResourceManager::collectPublish(std::vector<multi_call::ServerBizTopoInfo> &infoVec) {
    autil::ScopedLock lock(_publishLock);
    const auto &publishInfos = _biz->getPublishInfos();
    const auto &aliasNames = publishInfos.bizAliasNames;
    multi_call::ServerBizTopoInfo info;
    info.partCount = _partCount;
    info.partId = _partId;
    info.version = publishInfos.version;
    info.protocolVersion = 0;
    info.targetWeight = 100;
    if (!getPublishMetaMap(info.metas)) {
        return false;
    }
    if (!aliasNames.empty()) {
        for (const auto &publish : aliasNames) {
            info.bizName = publish;
            infoVec.push_back(info);
        }
    } else {
        info.bizName = _bizName;
        infoVec.push_back(info);
    }
     return true;
}

void ResourceManager::startPublish(multi_call::GigRpcServer *gigRpcServer) {
    {
        autil::ScopedLock lock(_publishLock);
        _gigRpcServer = gigRpcServer;
        if (!_gigRpcServer) {
            NAVI_KERNEL_LOG(INFO, "publish disabled, no gigRpcServer");
        }
        _oldResourceManager = nullptr;
    }
    updatePublishMeta();
}

bool ResourceManager::getPublishMetaMap(std::map<std::string, std::string> &metaMap) const {
    auto map = _mergedResourceMap.getMap();
    for (const auto &pair : map) {
        const auto &name = pair.first;
        const auto &resource = pair.second;
        std::string meta;
        if (!resource->getMeta(meta)) {
            continue;
        }
        metaMap[name] = meta;
    }
    if (!addStaticGraphMeta(metaMap)) {
        return false;
    }
    return true;
}

bool ResourceManager::addStaticGraphMeta(std::map<std::string, std::string> &metaMap) const {
    if (metaMap.end() != metaMap.find(NAVI_BUILDIN_STATIC_GRAPH_META)) {
        NAVI_KERNEL_LOG(ERROR, "buildin meta key [%s] exist in meta map", NAVI_BUILDIN_STATIC_GRAPH_META.c_str());
        return false;
    }
    if (!_staticGraphInfoMap) {
        return true;
    }
    std::map<std::string, std::string> metaInfoMap;
    for (const auto &pair : *_staticGraphInfoMap) {
        metaInfoMap[pair.first] = pair.second->meta;
    }
    auto metaStr = autil::legacy::FastToJsonString(metaInfoMap);
    metaMap[NAVI_BUILDIN_STATIC_GRAPH_META] = metaStr;
    return true;
}

void ResourceManager::collectResourceMap(
    MultiLayerResourceMap &multiLayerResourceMap) const
{
    NAVI_KERNEL_LOG(
        SCHEDULE1,
        "biz [%s] partCount [%d] partId [%d], collect from resource map [%s]",
        _bizName.c_str(), _partCount, _partId,
        autil::StringUtil::toString(_mergedResourceMap).c_str());
    multiLayerResourceMap.addResourceMap(&_mergedResourceMap);
}

ResourcePtr ResourceManager::buildKernelResourceRecur(const std::string &name,
                                                      KernelConfigContext *ctx,
                                                      const SubNamedDataMap *namedDataMap,
                                                      const ResourceMap &nodeResourceMap,
                                                      Node *requireKernelNode,
                                                      ResourceMap &inputResourceMap) const
{
    auto creator = _creatorManager.getResourceCreator(name);
    if (!creator) {
        NAVI_KERNEL_LOG(WARN, "resource [%s] not registered, biz: [%s]",
                        name.c_str(), _bizName.c_str());
        return nullptr;
    }
    auto resource = inputResourceMap.getResource(name);
    if (resource) {
        return resource;
    }
    resource = nodeResourceMap.getResource(name);
    if (resource) {
        return resource;
    }
    auto stage = creator->getStage();
    if (RS_KERNEL > stage) {
        NAVI_KERNEL_LOG(ERROR,
                        "can't build resource [%s] of stage [%s], biz: [%s]",
                        name.c_str(),
                        ResourceStage_Name(stage).c_str(),
                        _bizName.c_str());
        return nullptr;
    }
    auto *jsonConfig = _configMap.getConfig(name);
    ResourceConfigContext thisContext(_configPath, jsonConfig, nullptr);
    if (!ctx) {
        ctx = &thisContext;
    }
    void *baseAddr = nullptr;
    resource = newAndConfigResource(name, creator, ctx, baseAddr);
    if (!resource) {
        return nullptr;
    }
    auto &dependMap = resource->_dependMap;
    const auto &dependResources = creator->getDependResources();
    for (const auto &pair : dependResources) {
        const auto &dependName = pair.first;
        auto require = pair.second;
        auto dependResource = buildKernelResourceRecur(
            dependName, ctx, namedDataMap, nodeResourceMap, requireKernelNode, inputResourceMap);
        if (!dependResource && require) {
            NAVI_KERNEL_LOG(DEBUG, "build resource [%s] failed, biz: [%s]", dependName.c_str(), _bizName.c_str());
            return nullptr;
        }
        dependMap.addResource(dependName, dependResource);
    }
    auto ec = initResource(resource, baseAddr, namedDataMap, {}, creator, ctx, requireKernelNode);
    if (EC_NONE == ec) {
        inputResourceMap.addResource(resource);
        return resource;
    } else {
        NAVI_KERNEL_LOG(DEBUG,
                        "build resource [%s] failed, biz: [%s], ec: [%s]",
                        name.c_str(),
                        _bizName.c_str(),
                        CommonUtil::getErrorString(ec));
        return nullptr;
    }
}

ErrorCode ResourceManager::createResource(const std::string &name,
                                          ResourceStage minStage,
                                          const MultiLayerResourceMap &multiLayerResourceMap,
                                          const SubNamedDataMap *namedDataMap,
                                          const ProduceInfo &produceInfo,
                                          NaviConfigContext *ctx,
                                          bool checkReuse,
                                          Node *requireKernelNode,
                                          ResourcePtr &retResource) {
    auto resource = getExistResource(name, multiLayerResourceMap);
    if (resource) {
        if (!checkReuse || validateDepend(resource, multiLayerResourceMap)) {
            NAVI_KERNEL_LOG(TRACE3, "reuse resource [%s] [%p]", name.c_str(),
                            resource.get());
            retResource = resource;
            return EC_NONE;
        }
    }
    auto creator = _creatorManager.getResourceCreator(name);
    if (!creator) {
        NAVI_KERNEL_LOG(WARN, "resource [%s] not registered, biz: [%s]",
                        name.c_str(), _bizName.c_str());
        return EC_LACK_REGISTRY;
    }
    auto stage = creator->getStage();
    if (RS_EXTERNAL == stage) {
        NAVI_KERNEL_LOG(WARN,
                        "can't create external resource [%s], stage [%s], biz [%s]",
                        name.c_str(),
                        ResourceStage_Name(stage).c_str(),
                        _bizName.c_str());
        return EC_ILLEGAL_RESOURCE_STAGE;
    }
    if (RS_RUN_GRAPH_EXTERNAL == stage) {
        NAVI_KERNEL_LOG(DEBUG,
                        "can't create external resource [%s], stage [%s], biz [%s]",
                        name.c_str(),
                        ResourceStage_Name(stage).c_str(),
                        _bizName.c_str());
        return EC_ILLEGAL_RESOURCE_STAGE;
    }
    if (getTestMode() == TM_NOT_TEST && stage < minStage) {
        NAVI_KERNEL_LOG(ERROR,
                        "can't create resource [%s stage: %s] in stage [%s], biz [%s]",
                        name.c_str(),
                        ResourceStage_Name(stage).c_str(),
                        ResourceStage_Name(minStage).c_str(),
                        _bizName.c_str());
        return EC_ILLEGAL_RESOURCE_STAGE;
    }
    if (RS_KERNEL == stage && !ctx) {
        NAVI_KERNEL_LOG(ERROR,
                        "can't config resource [%s stage: %s], lack config context",
                        name.c_str(),
                        ResourceStage_Name(stage).c_str());
        return EC_RESOURCE_CONFIG;
    }
    auto *jsonConfig = _configMap.getConfig(name);
    ResourceConfigContext thisContext(_configPath, jsonConfig, nullptr);
    if (!ctx) {
        ctx = &thisContext;
    }
    void *baseAddr = nullptr;
    resource = newAndConfigResource(name, creator, ctx, baseAddr);
    if (!resource) {
        return EC_RESOURCE_CONFIG;
    }
    auto ec = collectDependMap(creator, multiLayerResourceMap, resource->_dependMap);
    if (EC_NONE != ec) {
        return ec;
    }
    ec = initResource(resource, baseAddr, namedDataMap, &produceInfo, creator, ctx, requireKernelNode);
    if (EC_NONE == ec) {
        retResource = resource;
    }
    return ec;
}

ErrorCode ResourceManager::collectDependMap(const ResourceCreator *creator,
                                            const MultiLayerResourceMap &multiLayerResourceMap,
                                            ResourceMap &dependResourceMap)
{
    bool lack = false;
    ResourceStage minLackStage = RS_UNKNOWN;
    const auto &dependResources = creator->getDependResources();
    for (const auto &pair : dependResources) {
        const auto &dependName = pair.first;
        auto require = pair.second;
        auto dependResource = getExistResource(dependName, multiLayerResourceMap);
        if (require && !dependResource) {
            lack = true;
            minLackStage = std::min(minLackStage, _creatorManager.getResourceStage(dependName));
            NAVI_KERNEL_LOG(WARN,
                            "create resource [%s] failed, lack required depend "
                            "resource [%s]",
                            creator->getName().c_str(), dependName.c_str());
            continue;
        }
        dependResourceMap.addResource(dependName, dependResource);
    }
    if (!lack) {
        return EC_NONE;
    }
    switch (minLackStage) {
    case RS_SNAPSHOT:
        return EC_LACK_RESOURCE_SNAPSHOT;
    case RS_BIZ:
        return EC_LACK_RESOURCE_BIZ;
    case RS_BIZ_PART:
        return EC_LACK_RESOURCE_BIZ_PART;
    case RS_GRAPH:
        return EC_LACK_RESOURCE_GRAPH;
    case RS_SUB_GRAPH:
        return EC_LACK_RESOURCE_SUB_GRAPH;
    case RS_SCOPE:
        return EC_LACK_RESOURCE_SCOPE;
    case RS_KERNEL:
        return EC_LACK_RESOURCE_KERNEL;
    case RS_RUN_GRAPH_EXTERNAL:
    case RS_EXTERNAL:
        return EC_LACK_RESOURCE_EXTERNAL;
    case RS_UNKNOWN:
        return EC_NONE;
    default:
        return EC_UNKNOWN;
    }
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

bool ResourceManager::validateResource(const ResourceStageMap &resourceStageMap) const {
    const auto &map = _resourceMap.getMap();
    for (const auto &pair : map) {
        const auto &name = pair.first;
        const auto &resource = pair.second;
        auto stage = _creatorManager.getResourceStage(name);
        if (stage >= RS_RUN_GRAPH_EXTERNAL) {
            NAVI_KERNEL_LOG(
                ERROR,
                "unknown error, can't cache resource [%s] [%p] stage [%s] in "
                "biz resource map, biz [%s] partCount [%d] partId [%d]",
                name.c_str(), resource.get(), ResourceStage_Name(stage).c_str(),
                _bizName.c_str(), _partCount, _partId);
            return false;
        }
        const auto &resourceInMerge = _mergedResourceMap.getResource(name);
        if (resourceInMerge != resource) {
            NAVI_KERNEL_LOG(
                ERROR,
                "unknown error, resource [%s] [%p] stage [%s] is not same in merged "
                "resource map [%p], biz [%s] partCount [%d] partId [%d]",
                name.c_str(), resource.get(), ResourceStage_Name(stage).c_str(),
                resourceInMerge.get(), _bizName.c_str(), _partCount, _partId);
            return false;
        }
    }
    const auto &mergedMap = _mergedResourceMap.getMap();
    for (const auto &pair : mergedMap) {
        const auto &name = pair.first;
        const auto &resource = pair.second;
        auto stage = _creatorManager.getResourceStage(name);
        if (stage >= RS_RUN_GRAPH_EXTERNAL) {
            NAVI_KERNEL_LOG(
                ERROR,
                "unknown error, can't cache resource [%s] [%p] stage [%s] in "
                "biz merge resource map, biz [%s] partCount [%d] partId [%d]",
                name.c_str(), resource.get(), ResourceStage_Name(stage).c_str(),
                _bizName.c_str(), _partCount, _partId);
            return false;
        }
    }
    if (!validateStage(resourceStageMap)) {
        return false;
    }
    return true;
}

bool ResourceManager::validateStage(const ResourceStageMap &resourceStageMap) const {
    for (const auto &stagePair : resourceStageMap) {
        auto stage = stagePair.first;
        const auto &stageMap = stagePair.second;
        bool needed = (stage < RS_RUN_GRAPH_EXTERNAL);
        for (const auto &resourcePair : stageMap) {
            const auto &name = resourcePair.first;
            auto require = resourcePair.second;
            bool has = _mergedResourceMap.hasResource(name);
            if (!needed) {
                if (has) {
                    NAVI_KERNEL_LOG(ERROR,
                                    "biz [%s] init failed, can't cache stage [%s] "
                                    "resource [%s] in biz resource list, partId [%d]",
                                    _bizName.c_str(),
                                    ResourceStage_Name(stage).c_str(),
                                    name.c_str(),
                                    _partId);
                    return false;
                }
            } else {
                if (require && !has) {
                    NAVI_KERNEL_LOG(ERROR,
                                    "biz [%s] init failed, lack required "
                                    "resource [%s] stage [%s], partId [%d], mergedMap [%s]",
                                    _bizName.c_str(),
                                    name.c_str(),
                                    ResourceStage_Name(stage).c_str(),
                                    _partId,
                                    autil::StringUtil::toString(_mergedResourceMap).c_str());
                    return false;
                }
            }
        }
    }
    return true;
}

bool ResourceManager::validateDepend(
    const ResourcePtr &resource,
    const MultiLayerResourceMap &multiLayerResourceMap) const
{
    const auto &dependMap = resource->getDepends().getMap();
    for (const auto &pair : dependMap) {
        const auto &depend = pair.second;
        if (!multiLayerResourceMap.hasResource(depend)) {
            NAVI_KERNEL_LOG(WARN,
                            "biz [%s] partCount [%d] partId [%d], validate resource failed "
                            "[%s] [%p], resourceMap [%s], lack depend [%s]",
                            _bizName.c_str(),
                            _partCount,
                            _partId,
                            pair.first.c_str(),
                            resource.get(),
                            autil::StringUtil::toString(multiLayerResourceMap).c_str(),
                            depend ? depend->getName().c_str() : "");
            return false;
        }
    }
    return true;
}

ResourcePtr ResourceManager::getResourceFromOld(const std::string &name) const {
    if (!_oldResourceManager) {
        return nullptr;
    }
    auto resource = _oldResourceManager->_mergedResourceMap.getResource(name);
    if (!resource) {
        return nullptr;
    }
    // can reuse?
    return nullptr;
}

ResourcePtr ResourceManager::newAndConfigResource(const std::string &name,
                                                  const ResourceCreator *creator,
                                                  NaviConfigContext *ctx,
                                                  void *&baseAddr) const {
    ResourcePtr resource = creator->create(baseAddr);
    assert(resource);
    try {
        if (resource->config(*ctx)) {
            return resource;
        } else {
            NAVI_KERNEL_LOG(
                WARN, "config resource [%s] failed, config ctx [%s], biz: %s",
                name.c_str(),
                autil::StringUtil::toString(*ctx).c_str(),
                _bizName.c_str());
            return nullptr;
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_KERNEL_LOG(WARN,
                        "config resource [%s] failed, exception [%s], config ctx [%s], biz: %s",
                        name.c_str(),
                        NAVI_TLS_LOGGER->logger->getExceptionMessage(e).c_str(),
                        autil::StringUtil::toString(*ctx).c_str(),
                        _bizName.c_str());
        return nullptr;
    }
}

ErrorCode ResourceManager::initResource(const ResourcePtr &resourcePtr,
                                        void *baseAddr,
                                        const SubNamedDataMap *namedDataMap,
                                        const ProduceInfo *produceInfo,
                                        const ResourceCreator *creator,
                                        NaviConfigContext *configCtx,
                                        Node *requireKernelNode) const
{
    const auto &bindInfos = creator->getBindInfos();
    const auto &dynamicInfoMap = creator->getDynamicGroupInfoMap();
    auto &resource = *resourcePtr;
    const auto &name = resource.getName();
    if (!bindResource(name, baseAddr, resource._dependMap, dynamicInfoMap,
                      bindInfos, {}, "resource"))
    {
        NAVI_KERNEL_LOG(
            WARN, "init resource [%s] failed, bind depend resource failed",
            name.c_str());
        return EC_BIND_RESOURCE;
    }
    if (!bindNamedData(name, resource, namedDataMap, bindInfos, "resource")) {
        NAVI_KERNEL_LOG(WARN,
                        "init resource [%s] failed, bind named data failed",
                        name.c_str());
        return EC_BIND_NAMED_DATA;
    }
    ProduceNotifierPtr notifier;
    if (produceInfo && produceInfo->_produce) {
        notifier = std::make_shared<ProduceNotifier>();
        notifier->_name = name;
        notifier->_info = *produceInfo;
        notifier->_resource = resourcePtr.get();
    }
    ResourceInitContext ctx(this,
                            creator,
                            _configPath,
                            _bizName,
                            _partCount,
                            _partId,
                            &resource._dependMap,
                            notifier,
                            *configCtx,
                            namedDataMap,
                            requireKernelNode);
    ErrorCode ec = EC_NONE;
    try {
        ec = resource.init(ctx);
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_KERNEL_LOG(WARN,
                        "init resource failed, resource [%s], biz: %s, exception: [%s]",
                        name.c_str(),
                        _bizName.c_str(),
                        e.GetMessage().c_str());
        ec = EC_ABORT;
    }
    if (EC_NONE != ec) {
        NAVI_KERNEL_LOG(WARN, "init resource failed, resource [%s], biz: %s",
                        name.c_str(), _bizName.c_str());
        return ec;
    }
    NAVI_KERNEL_LOG(TRACE3, "create resource [%s] success: %p, biz: %s",
                    name.c_str(), resourcePtr.get(), _bizName.c_str());
    return EC_NONE;
}

DataPtr ResourceManager::getNamedData(const SubNamedDataMap *namedDataMap,
                                      const std::string &name)
{
    if (!namedDataMap) {
        return nullptr;
    }
    auto it = namedDataMap->find(name);
    if (namedDataMap->end() == it) {
        return nullptr;
    }
    return it->second.data;
}

NaviPartId ResourceManager::getPartCount() const {
    return _partCount;
}

bool ResourceManager::hasConfig(const std::string &name) const {
    return _configMap.hasConfig(name);
}

void ResourceManager::fillSummary(PartSummary &partSummary) const {
    const auto &map = _resourceMap.getMap();
    for (const auto &pair : map) {
        const auto &name = pair.first;
        auto stage = _creatorManager.getResourceStage(name);
        ResourceLoadInfo info;
        info.stage = ResourceStage_Name(stage);
        partSummary.resourceSet.infos.emplace(name, std::move(info));
    }
    std::shared_ptr<std::unordered_map<std::string, StaticGraphInfoPtr>> staticGraphInfoMap;
    {
        autil::ScopedLock lock(_publishLock);
        staticGraphInfoMap = _staticGraphInfoMap;
    }
    if (staticGraphInfoMap) {
        for (const auto &pair : *staticGraphInfoMap) {
            partSummary.staticGraphSummary[pair.first] = pair.second->meta;
        }
    }
}

TestMode ResourceManager::getTestMode() const { return _biz->getTestMode(); }

}
