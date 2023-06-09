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
#include "navi/engine/BizManager.h"
#include "navi/log/NaviLogger.h"
#include "navi/ops/ResourceKernelDef.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "navi/resource/MetaInfoResourceBase.h"
#include "autil/StringUtil.h"

namespace navi {

BizManager::BizManager(const std::shared_ptr<CreatorManager> &creatorManager)
    : _creatorManager(creatorManager)
    , _graphDef(nullptr)
{
}

BizManager::~BizManager() {
    _bizMap.clear();
    if (_graphDef) {
        DELETE_AND_SET_NULL(_graphDef);
    }
}

bool BizManager::preInit(const NaviLoggerPtr &logger,
                         BizManager *oldBizManager,
                         const NaviConfig &config,
                         const ResourceMap &rootResourceMap)
{
    _logger.object = this;
    _logger.logger = logger;
    _logger.addPrefix("biz manager");
    NaviLoggerScope scope(_logger);
    if (!initCreatorManager(config)) {
        return false;
    }
    if (!createDefaultBiz(config)) {
        return false;
    }
    if (!saveMemoryPoolResource(rootResourceMap)) {
        return false;
    }
    if (!createBizs(config, oldBizManager)) {
        return false;
    }
    if (!initResourceGraph(config, rootResourceMap)) {
        return false;
    }
    return true;
}

GraphDef *BizManager::stealResourceGraph() {
    auto graphDef = _graphDef;
    _graphDef = nullptr;
    return graphDef;
}

bool BizManager::postInit() {
    for (const auto &pair : _bizMap) {
        if (!pair.second->postInit(*_defaultBiz)) {
            return false;
        }
    }
    _defaultBiz->collectResourceMap(1, 0, _rootMultiLayerResourceMap);
    return true;
}

bool BizManager::initCreatorManager(const NaviConfig &config) {
    if (_creatorManager) {
        return true;
    }
    _creatorManager.reset(new CreatorManager());
    if (!_creatorManager->init(config)) {
        return false;
    }
    return true;
}

bool BizManager::createDefaultBiz(const NaviConfig &config) {
    _defaultConfig.partCount = 1;
    _defaultConfig.partIds.push_back(0);
    _defaultConfig.resourceConfigVec = config.buildinResourceConfigVec;
    initDefaultBizResourceList(config.buildinResourceConfigVec, config.buildinInitResources);
    _defaultBiz = createBiz(NAVI_DEFAULT_BIZ, "", _defaultConfig, nullptr);
    if (!_defaultBiz) {
        return false;
    }
    _bizMap.emplace(NAVI_DEFAULT_BIZ, _defaultBiz);
    return true;
}

void BizManager::initDefaultBizResourceList(const std::vector<NaviRegistryConfig> &configVec,
                                            const std::set<std::string> &initResources) {
    _defaultConfig.initResources.insert(initResources.begin(), initResources.end());
    for (const auto &conf : configVec) {
        if (conf.name == GIG_CLIENT_RESOURCE_ID) {
            _defaultConfig.initResources.insert(GIG_CLIENT_RESOURCE_ID);
            break;
        }
    }
    auto rootResources = _creatorManager->getResourceByLoadType(RLT_BUILDIN);
    for (const auto &resourceName : rootResources) {
        NAVI_KERNEL_LOG(INFO, "register RLT_BUILDIN resource: %s",
                        resourceName.c_str());
        _defaultConfig.initResources.insert(resourceName);
    }
}

bool BizManager::saveMemoryPoolResource(const ResourceMap &rootResourceMap) {
    auto memoryPoolResource = rootResourceMap.getResource(MEMORY_POOL_RESOURCE_ID);
    if (!memoryPoolResource) {
        NAVI_KERNEL_LOG(ERROR, "get resource[%s] failed", MEMORY_POOL_RESOURCE_ID.c_str());
        return false;
    }
    _memoryPoolResourceMap.addResource(memoryPoolResource);
    _rootMultiLayerResourceMap.addResourceMap(&_memoryPoolResourceMap);
    return true;
}

GraphMemoryPoolResourcePtr BizManager::createGraphMemoryPoolResource() const {
    auto resource = _defaultBiz->createResource(
        1, 0, GRAPH_MEMORY_POOL_RESOURCE_ID, _rootMultiLayerResourceMap);
    return std::dynamic_pointer_cast<GraphMemoryPoolResource>(resource);
}

MetaInfoResourceBasePtr BizManager::createMetaInfoResource() const {
    auto resource = _defaultBiz->createResource(
        1, 0, META_INFO_RESOURCE_ID, _rootMultiLayerResourceMap);
    if (!resource) {
        NAVI_KERNEL_LOG(DEBUG, "skip create meta info resource");
        return nullptr;
    }
    auto metaInfoResource =std::dynamic_pointer_cast<MetaInfoResourceBase>(resource);
    if (!metaInfoResource) {
        auto &ref = *resource;
        NAVI_KERNEL_LOG(ERROR, "cast to meta info resource failed, typeid [%s]",
                        typeid(ref).name());
        return nullptr;
    }
    return metaInfoResource;
}

bool BizManager::createBizs(const NaviConfig &config,
                            BizManager *oldBizManager)
{
    for (const auto &pair : config.bizMap) {
        const auto &bizName = pair.first;
        const auto &bizConfig = pair.second;
        const Biz *oldBiz = nullptr;
        if (oldBizManager) {
            oldBiz = oldBizManager->getBiz(_logger.logger.get(), bizName).get();
        }
        auto biz = createBiz(bizName, config.configPath, bizConfig, oldBiz);
        if (!biz) {
            return false;
        }
        _bizMap.emplace(bizName, std::move(biz));
    }
    return true;
}

BizPtr BizManager::createBiz(const std::string &bizName,
                             const std::string &configPath,
                             const NaviBizConfig &config,
                             const Biz *oldBiz)
{
    std::unique_ptr<Biz> biz(new Biz(this, bizName, *_creatorManager));
    if (biz->preInit(configPath, config, oldBiz)) {
        return BizPtr(biz.release());
    } else {
        return nullptr;
    }
}

bool BizManager::initResourceGraph(const NaviConfig &config,
                                   const ResourceMap &rootResourceMap)
{
    try {
        MultiLayerResourceMap multiLayerResourceMap;
        multiLayerResourceMap.addResourceMap(&rootResourceMap);
        std::unique_ptr<GraphDef> def(new GraphDef());
        GraphBuilder builder(def.get());
        auto defaultGraphId = initDefaultGraph(multiLayerResourceMap, builder);
        bool hasInitResource = (INVALID_GRAPH_ID != defaultGraphId);
        for (const auto &pair : config.bizMap) {
            const auto &bizName = pair.first;
            const auto &bizConf = pair.second;
            auto partCount = bizConf.partCount;
            const auto &partIds = bizConf.partIds;
            auto initResources = bizConf.initResources;
            if (rootResourceMap.hasResource(ROOT_KMON_RESOURCE_ID)) {
                NAVI_KERNEL_LOG(
                    INFO, "add init resource [%s] for biz [%s]", BIZ_KMON_RESOURCE_ID.c_str(), bizName.c_str());
                initResources.emplace(BIZ_KMON_RESOURCE_ID);
            }
            if (initResources.empty()) {
                continue;
            }
            auto biz = doGetBiz(bizName);
            assert(biz);
            hasInitResource = true;
            for (auto partId : partIds) {
                auto graphId = builder.newSubGraph(bizName);
                builder.subGraph(graphId).location(bizName, partCount, partId);
                builder.node(RESOURCE_SAVE_KERNEL)
                    .kernel(RESOURCE_SAVE_KERNEL)
                    .out(RESOURCE_SAVE_KERNEL_OUTPUT)
                    .asGraphOutput(bizName + ":" + autil::StringUtil::toString(partId));
                for (const auto &resource : initResources) {
                    addResourceGraphWithDefault(biz.get(), resource, defaultGraphId, multiLayerResourceMap, builder);
                }
            }
        }
        if (hasInitResource) {
            _graphDef = def.release();
        }
        return true;
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_KERNEL_LOG(ERROR, "init resource graph failed, msg [%s]", e.GetMessage().c_str());
        return false;
    }
}

GraphId BizManager::initDefaultGraph(
        const MultiLayerResourceMap &multiLayerResourceMap,
        GraphBuilder &builder)
{
    const auto &resources = _defaultConfig.initResources;
    if (resources.empty()) {
        return INVALID_GRAPH_ID;
    }
    const auto &bizName = NAVI_DEFAULT_BIZ;
    auto graphId = builder.newSubGraph(bizName);
    builder.subGraph(graphId)
        .location(bizName, 1, 0);
    builder.node(RESOURCE_SAVE_KERNEL)
        .kernel(RESOURCE_SAVE_KERNEL)
        .out(RESOURCE_SAVE_KERNEL_OUTPUT).asGraphOutput(bizName + ":0");
    for (const auto &resource : resources) {
        addResourceGraphWithDefault(_defaultBiz.get(), resource,
                                    INVALID_GRAPH_ID, multiLayerResourceMap,
                                    builder);
    }
    return graphId;
}

void BizManager::addResourceGraph(const Biz *biz,
                                  const std::string &resource,
                                  const MultiLayerResourceMap &multiLayerResourceMap,
                                  GraphBuilder &builder) const
{
    if (builder.hasNode(resource)) {
        builder.node(resource)
            .integerAttr(RESOURCE_ATTR_REQUIRE, 1);
        return;
    }
    NAVI_KERNEL_LOG(SCHEDULE2, "add resource create [%s]", resource.c_str());
    builder.resourceNode(resource)
        .kernel(RESOURCE_CREATE_KERNEL)
        .integerAttr(RESOURCE_ATTR_REQUIRE, 1)
        .attr(RESOURCE_ATTR_NAME, resource);
    if (multiLayerResourceMap.hasResource(resource)) {
        return;
    }
    addDependResourceRecur(biz, resource, multiLayerResourceMap, builder);
}

void BizManager::addDependResourceRecur(
    const Biz *biz,
    const std::string &resource,
    const MultiLayerResourceMap &multiLayerResourceMap,
    GraphBuilder &builder) const
{
    auto dependResourceMapPtr = biz->getResourceDependResourceMap(resource);
    if (!dependResourceMapPtr) {
        NAVI_KERNEL_LOG(DEBUG, "resource [%s] not registered",
                        resource.c_str());
        return;
    }
    auto resourceInputPort = builder.node(resource).in(NODE_RESOURCE_INPUT_PORT);
    for (const auto &pair : *dependResourceMapPtr) {
        const auto &dependName = pair.first;
        if (multiLayerResourceMap.hasResource(dependName)) {
            continue;
        }
        auto nodeExist = builder.hasNode(dependName);
        if (!nodeExist) {
            builder.resourceNode(dependName)
                .kernel(RESOURCE_CREATE_KERNEL)
                .attr(RESOURCE_ATTR_NAME, dependName)
                .out(RESOURCE_CREATE_OUTPUT)
                .to(resourceInputPort.autoNext());
            NAVI_KERNEL_LOG(DEBUG, "link resource [%s] depend [%s]",
                            resource.c_str(), dependName.c_str());
            addDependResourceRecur(biz, dependName, multiLayerResourceMap, builder);
        } else {
            builder.node(dependName).out(RESOURCE_CREATE_OUTPUT).to(resourceInputPort.autoNext());
            NAVI_KERNEL_LOG(DEBUG, "resource [%s] depend [%s] already exist",
                            resource.c_str(), dependName.c_str());
        }
    }
}

void BizManager::addResourceGraphWithDefault(
        const Biz *biz,
        const std::string &resource,
        GraphId defaultGraphId,
        const MultiLayerResourceMap &multiLayerResourceMap,
        GraphBuilder &builder) const
{
    if (builder.hasNode(resource)) {
        builder.node(resource)
            .integerAttr(RESOURCE_ATTR_REQUIRE, 1);
        return;
    }
    auto savePort = builder.node(RESOURCE_SAVE_KERNEL).in(RESOURCE_SAVE_KERNEL_INPUT);
    auto defaultHasNode = builder.hasNode(defaultGraphId, resource);
    if (defaultHasNode) {
        auto createPort = builder.node(defaultGraphId, resource).out(RESOURCE_CREATE_OUTPUT);
        savePort.autoNext().from(createPort);
        return;
    }
    NAVI_KERNEL_LOG(DEBUG, "add resource create [%s]", resource.c_str());
    builder.resourceNode(resource)
        .kernel(RESOURCE_CREATE_KERNEL)
        .integerAttr(RESOURCE_ATTR_REQUIRE, 1)
        .attr(RESOURCE_ATTR_NAME, resource)
        .out(RESOURCE_CREATE_OUTPUT)
        .to(savePort.autoNext());
    if (multiLayerResourceMap.hasResource(resource)) {
        return;
    }
    addDependResourceWithDefaultRecur(biz, resource, defaultGraphId,
                                      multiLayerResourceMap, builder);
}

void BizManager::addDependResourceWithDefaultRecur(
    const Biz *biz,
    const std::string &resource,
    GraphId defaultGraphId,
    const MultiLayerResourceMap &multiLayerResourceMap,
    GraphBuilder &builder) const
{
    auto dependResourceMapPtr = biz->getResourceDependResourceMap(resource);
    if (!dependResourceMapPtr) {
        NAVI_KERNEL_LOG(DEBUG, "resource [%s] not registered",
                        resource.c_str());
        return;
    }
    auto resourceInputPort = builder.node(resource).in(NODE_RESOURCE_INPUT_PORT);
    auto savePort = builder.node(RESOURCE_SAVE_KERNEL).in(RESOURCE_SAVE_KERNEL_INPUT);
    for (const auto &pair : *dependResourceMapPtr) {
        const auto &dependName = pair.first;
        if (multiLayerResourceMap.hasResource(dependName)) {
            continue;
        }
        auto thisHasNode = builder.hasNode(dependName);
        if (!thisHasNode) {
            auto defaultHasNode = builder.hasNode(defaultGraphId, dependName);
            if (defaultHasNode) {
                auto dependPort = builder.node(defaultGraphId, dependName).out(RESOURCE_CREATE_OUTPUT);
                dependPort.to(resourceInputPort.autoNext());
                dependPort.to(savePort.autoNext());
            } else {
                auto dependPort = builder.resourceNode(dependName)
                                      .kernel(RESOURCE_CREATE_KERNEL)
                                      .attr(RESOURCE_ATTR_NAME, dependName)
                                      .out(RESOURCE_CREATE_OUTPUT);
                dependPort.to(resourceInputPort.autoNext());
                dependPort.to(savePort.autoNext());
                NAVI_KERNEL_LOG(DEBUG, "add resource [%s] depend [%s]", resource.c_str(), dependName.c_str());
                addDependResourceWithDefaultRecur(biz, dependName, defaultGraphId, multiLayerResourceMap, builder);
            }
        } else {
            builder.node(dependName).out(RESOURCE_CREATE_OUTPUT).to(resourceInputPort.autoNext());
            NAVI_KERNEL_LOG(DEBUG, "resource [%s] depend [%s] already exist",
                            resource.c_str(), dependName.c_str());
        }
    }
}

GraphDef *BizManager::createResourceGraph(
        const std::string &bizName,
        NaviPartId partCount,
        NaviPartId partId,
        const std::set<std::string> &resourceSet) const
{
    auto biz = doGetBiz(bizName);
    if (!biz) {
        NAVI_KERNEL_LOG(ERROR, "biz [%s] not exist", bizName.c_str());
        return nullptr;
    }
    MultiLayerResourceMap multiLayerResourceMap;
    biz->collectResourceMap(partCount, partId, multiLayerResourceMap);
    NAVI_KERNEL_LOG(SCHEDULE1,
                    "collected resource map [%s], biz [%s] partCount [%d], partId [%d]",
                    autil::StringUtil::toString(multiLayerResourceMap).c_str(),
                    bizName.c_str(), partCount, partId);
    assert(!resourceSet.empty());
    try {
        std::unique_ptr<GraphDef> def(new GraphDef());
        GraphBuilder builder(def.get());
        auto graphId = builder.newSubGraph(bizName);
        builder.subGraph(graphId).location(bizName, partCount, partId);
        for (const auto &resourceName : resourceSet) {
            addResourceGraph(biz.get(), resourceName, multiLayerResourceMap, builder);
        }
        const auto &subGraph = def->sub_graphs(graphId);
        auto nodeCount = subGraph.nodes_size();
        std::vector<std::string> fullResourceVec;
        for (int32_t i = 0; i < nodeCount; i++) {
            const auto &nodeDef = subGraph.nodes(i);
            if (NT_RESOURCE == nodeDef.type()) {
                fullResourceVec.emplace_back(nodeDef.name());
            }
        }
        for (const auto &resourceName : fullResourceVec) {
            builder.node(graphId, resourceName).out(RESOURCE_CREATE_OUTPUT).asGraphOutput(resourceName);
        }
        return def.release();
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_KERNEL_LOG(ERROR, "create resource graph failed, msg [%s]", e.GetMessage().c_str());
        return nullptr;
    }
}

const ResourceCreator *BizManager::getResourceCreator(
        const std::string &name) const
{
    return _creatorManager->getResourceCreator(name);
}

bool BizManager::isLocal(const LocationDef &location) const {
    const auto &bizName = location.biz_name();
    auto biz = doGetBiz(bizName);
    if (!biz) {
        return false;
    }
    auto thisPartId = location.this_part_id();
    if (INVALID_NAVI_PART_ID == thisPartId) {
        // input graph from user
        if (0 == location.part_info().part_ids_size()) {
            return biz->isSinglePart();
        } else {
            for (auto part_id : location.part_info().part_ids()) {
                if (!biz->hasPartId(part_id)) {
                    return false;
                }
            }
            return true;
        }
    } else {
        // internal server graph
        return biz->hasPartId(thisPartId);
    }
}

BizPtr BizManager::getBiz(NaviLogger *_logger, const std::string &bizName) const {
    auto biz = doGetBiz(bizName);
    if (!biz) {
        NAVI_LOG(ERROR, "biz [%s] not exist", bizName.c_str());
        return _defaultBiz;
    } else {
        return biz;
    }
}

BizPtr BizManager::doGetBiz(const std::string &bizName) const {
    auto it = _bizMap.find(bizName);
    if (it != _bizMap.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

bool BizManager::isDefaultBiz(const BizPtr &biz) const {
    return biz == _defaultBiz;
}

const CreatorManagerPtr &BizManager::getCreatorManager() const {
    return _creatorManager;
}

ResourcePtr BizManager::getRootResource(const std::string &name) const {
    return _rootMultiLayerResourceMap.getResource(name);
}

}
