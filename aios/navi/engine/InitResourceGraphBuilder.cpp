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
#include "navi/engine/InitResourceGraphBuilder.h"
#include "navi/ops/ResourceKernelDef.h"

namespace navi {

InitResourceGraphBuilder::InitResourceGraphBuilder(
        const std::string &bizName, CreatorManager *creatorManager,
        const ResourceStageMap &resourceStageMap, NaviPartId partCount,
        const std::vector<NaviPartId> &partIds, GraphBuilder &builder)
    : _bizName(bizName)
    , _isBuildin(NAVI_BUILDIN_BIZ == bizName)
    , _creatorManager(creatorManager)
    , _resourceStageMap(resourceStageMap)
    , _partCount(partCount)
    , _partIds(partIds)
    , _builder(builder)
    , _buildinGraphId(INVALID_GRAPH_ID)
    , _bizGraphId(INVALID_GRAPH_ID)
    , _hasError(false)
{
}

InitResourceGraphBuilder::~InitResourceGraphBuilder() {
}

bool InitResourceGraphBuilder::build(GraphId buildinGraphId,
                                     GraphId &resultGraphId)
{
    if (INVALID_GRAPH_ID != buildinGraphId) {
        _graphInfoMap[buildinGraphId] = { RS_SNAPSHOT, INVALID_NAVI_PART_ID };
    }
    _buildinGraphId = buildinGraphId;
    for (const auto &stagePair : _resourceStageMap) {
        auto stage = stagePair.first;
        if (_isBuildin && RS_SNAPSHOT != stage) {
            continue;
        }
        const auto &stageMap = stagePair.second;
        for (const auto &resourcePair : stageMap) {
            const auto &name = resourcePair.first;
            auto require = resourcePair.second;
            if (!addResourceToGraph(name, require, {})) {
                return false;
            }
        }
    }
    resultGraphId = _buildinGraphId;
    return (!_hasError);
}

bool InitResourceGraphBuilder::addResourceToGraph(
    const std::string &name, bool require, const RequireNodeInfo &requireInfo)
{
    GraphId newGraphId = INVALID_GRAPH_ID;
    auto creator = _creatorManager->getResourceCreator(name);
    if (!creator) {
        NAVI_KERNEL_LOG(ERROR, "add resource [%s] to graph failed, not registered", name.c_str());
        return false;
    }
    auto stage = creator->getStage();
    validate(creator, requireInfo);
    if (_isBuildin) {
        newGraphId = addBuildinSubGraph();
    } else {
        switch (stage) {
        case RS_SNAPSHOT:
            newGraphId = addBuildinSubGraph();
            break;
        case RS_BIZ:
            newGraphId = addBizSubGraph();
            break;
        case RS_BIZ_PART:
            addBizPartSubGraph();
            break;
        case RS_RUN_GRAPH_EXTERNAL:
        case RS_GRAPH:
        case RS_SUB_GRAPH:
        case RS_SCOPE:
        case RS_KERNEL:
        case RS_EXTERNAL:
            newGraphId = addRunGraphSubGraph(requireInfo.graphId);
            break;
        default:
            NAVI_KERNEL_LOG(INFO, "add resource [%s] to graph failed, not registered", name.c_str());
            return false;
        }
    }
    const auto &producer = creator->getProducer();
    if (INVALID_GRAPH_ID == newGraphId) {
        for (const auto &pair : _partGraphIds) {
            auto graphId = pair.second;
            if (!addResource(graphId, name, producer, require, requireInfo)) {
                return false;
            }
        }
    } else {
        if (!addResource(newGraphId, name, producer, require, requireInfo)) {
            return false;
        }
    }
    return true;
}

bool InitResourceGraphBuilder::addResource(GraphId graphId,
                                           const std::string &name,
                                           const std::string &producer,
                                           bool require,
                                           const RequireNodeInfo &requireInfo)
{
    bool isProduced = (!producer.empty());
    std::string nodeName = isProduced ? producer : name;
    std::string portName = isProduced ? RESOURCE_PRODUCE_OUTPUT : RESOURCE_CREATE_OUTPUT;
    std::string requireAttr = isProduced ? RESOURCE_ATTR_PRODUCE_REQUIRE : RESOURCE_ATTR_REQUIRE;

    _builder.subGraph(graphId);
    if (_builder.hasNode(nodeName)) {
        auto n = _builder.resourceNode(nodeName);
        if (require) {
            n.integerAttr(requireAttr, 1);
        }
        if (isProduced) {
            n.attr(RESOURCE_ATTR_PRODUCE_NAME, name);
        }
        auto port = n.out(portName);
        if (!port.hasConnected()) {
            if (isProduced) {
                addFlushInput(n);
            }
            auto saveInput = _builder.node(RESOURCE_SAVE_KERNEL).in(RESOURCE_SAVE_KERNEL_INPUT);
            port.to(saveInput.autoNext());
        }
        if (requireInfo.graphId != INVALID_GRAPH_ID) {
            auto destPort = _builder.resourceNode(requireInfo.graphId, requireInfo.node).in(NODE_RESOURCE_INPUT_PORT);
            port.to(destPort.autoNext());
        }
        return true;
    }
    auto saveInput = _builder.node(RESOURCE_SAVE_KERNEL).in(RESOURCE_SAVE_KERNEL_INPUT);
    NAVI_KERNEL_LOG(SCHEDULE2,
                    "add resource create [%s], graphId: %d, require node [%s]",
                    nodeName.c_str(),
                    _builder.currentSubGraph(),
                    requireInfo.node.c_str());
    auto n = _builder.resourceNode(nodeName)
                 .kernel(RESOURCE_CREATE_KERNEL)
                 .integerAttr(requireAttr, require)
                 .integerAttr(RESOURCE_ATTR_INIT, true)
                 .attr(RESOURCE_ATTR_NAME, nodeName);
    if (isProduced) {
        n.attr(RESOURCE_ATTR_PRODUCE_NAME, name);
        addFlushInput(n);
    }
    auto outputPort = n.out(portName);
    outputPort.to(saveInput.autoNext());
    if (requireInfo.graphId != INVALID_GRAPH_ID) {
        auto destPort = _builder.resourceNode(requireInfo.graphId, requireInfo.node).in(NODE_RESOURCE_INPUT_PORT);
        outputPort.to(destPort.autoNext());
    }
    if (!addReplaceResource(graphId, nodeName)) {
        return false;
    }
    return addDependResource(graphId, nodeName);
}

void InitResourceGraphBuilder::addFlushInput(N n) {
    addBuildinSubGraph();
    auto flush = _builder.node(_buildinGraphId, RESOURCE_FLUSH_KERNEL).out(RESOURCE_FLUSH_KERNEL_OUTPUT);
    auto inPort = n.in(NODE_RESOURCE_INPUT_PORT);
    flush.to(inPort.autoNext());
}

bool InitResourceGraphBuilder::addDependResource(GraphId graphId,
                                                 const std::string &name)
{
    auto dependResourceMapPtr =
        _creatorManager->getResourceDependResourceMap(name);
    if (!dependResourceMapPtr) {
        NAVI_KERNEL_LOG(DEBUG, "resource [%s] not registered", name.c_str());
        return true;
    }
    RequireNodeInfo requireInfo;
    requireInfo.graphId = graphId;
    requireInfo.node = name;
    for (const auto &pair : *dependResourceMapPtr) {
        const auto &dependName = pair.first;
        if (!addResourceToGraph(dependName, false, requireInfo)) {
            return false;
        }
    }
    return true;
}

bool InitResourceGraphBuilder::addReplaceResource(GraphId graphId, const std::string &name) {
    auto creator = _creatorManager->getResourceCreator(name);
    if (!creator) {
        NAVI_KERNEL_LOG(DEBUG, "resource [%s] not registered", name.c_str());
        return true;
    }
    RequireNodeInfo requireInfo;
    requireInfo.graphId = graphId;
    requireInfo.node = name;
    const auto &replaceBySet = creator->getReplaceBySet();
    for (const auto &replacerName : replaceBySet) {
        if (!addResourceToGraph(replacerName, false, requireInfo)) {
            return false;
        }
    }
    return true;
}

GraphId InitResourceGraphBuilder::addBuildinSubGraph() {
    if (INVALID_GRAPH_ID != _buildinGraphId) {
        return _buildinGraphId;
    }
    const auto &bizName = NAVI_BUILDIN_BIZ;
    auto buildinGraphId = _builder.newSubGraph(bizName);
    _builder.subGraph(buildinGraphId)
        .location(bizName, 1, 0)
        .node(RESOURCE_SAVE_KERNEL)
        .kernel(RESOURCE_SAVE_KERNEL)
        .out(RESOURCE_SAVE_KERNEL_OUTPUT)
        .asGraphOutput(bizName + ":-1");
    _buildinGraphId = buildinGraphId;
    _graphInfoMap[buildinGraphId] = { RS_SNAPSHOT, INVALID_NAVI_PART_ID };
    NAVI_KERNEL_LOG(SCHEDULE2, "biz [%s] resource graphId: %d", bizName.c_str(),
                    buildinGraphId);
    _builder.node(RESOURCE_FLUSH_KERNEL)
        .kernel(RESOURCE_FLUSH_KERNEL);
    return _buildinGraphId;
}

GraphId InitResourceGraphBuilder::addBizSubGraph() {
    if (INVALID_GRAPH_ID != _bizGraphId) {
        return _bizGraphId;
    }
    auto bizGraphId = _builder.newSubGraph(_bizName);
    _builder.subGraph(bizGraphId)
        .location(_bizName, _partCount, NAVI_BIZ_PART_ID)
        .node(RESOURCE_SAVE_KERNEL)
        .kernel(RESOURCE_SAVE_KERNEL)
        .out(RESOURCE_SAVE_KERNEL_OUTPUT)
        .asGraphOutput(_bizName + ":" + autil::StringUtil::toString(NAVI_BIZ_PART_ID));
    _bizGraphId = bizGraphId;
    _graphInfoMap[bizGraphId] = { RS_BIZ, INVALID_NAVI_PART_ID };
    NAVI_KERNEL_LOG(SCHEDULE2, "biz [%s] resource graphId: %d",
                    _bizName.c_str(), bizGraphId);
    addSaveToPublish(_bizGraphId);
    return _bizGraphId;
}

void InitResourceGraphBuilder::addBizPartSubGraph() {
    if (!_partGraphIds.empty()) {
        return;
    }
    for (auto partId : _partIds) {
        auto partGraphId = _builder.newSubGraph(_bizName);
        auto saveOut = _builder.subGraph(partGraphId)
                           .location(_bizName, _partCount, partId)
                           .node(RESOURCE_SAVE_KERNEL)
                           .kernel(RESOURCE_SAVE_KERNEL)
                           .out(RESOURCE_SAVE_KERNEL_OUTPUT);
        if (_isBuildin) {
            saveOut.asGraphOutput(_bizName + ":" + autil::StringUtil::toString(partId));
        } else {
            auto publishNode = _builder.subGraph(partGraphId)
                                   .location(_bizName, _partCount, partId)
                                   .node(RESOURCE_PUBLISH_KERNEL)
                                   .kernel(RESOURCE_PUBLISH_KERNEL);
            publishNode.in(RESOURCE_PUBLISH_KERNEL_INPUT).autoNext().from(saveOut);
            publishNode.out(RESOURCE_PUBLISH_KERNEL_OUTPUT)
                .asGraphOutput(_bizName + ":" + autil::StringUtil::toString(partId));
        }
        _partGraphIds[partId] = partGraphId;
        _graphInfoMap[partGraphId] = { RS_BIZ_PART, partId };
        NAVI_KERNEL_LOG(SCHEDULE2, "biz [%s] partId [%d] resource graphId: %d",
                        _bizName.c_str(), partId, partGraphId);
    }
    if (_isBuildin) {
        return;
    }
    addSaveToPublish(_buildinGraphId);
    addSaveToPublish(_bizGraphId);
}

GraphId InitResourceGraphBuilder::addRunGraphSubGraph(GraphId requireGraph) {
    auto graphInfo = getGraphInfo(requireGraph);
    switch (graphInfo.stage) {
    case RS_SNAPSHOT:
        addBuildinSubGraph();
        return _buildinGraphId;
    case RS_BIZ:
        addBizSubGraph();
        return _bizGraphId;
    case RS_BIZ_PART:
    case RS_RUN_GRAPH_EXTERNAL:
    case RS_GRAPH:
    case RS_SUB_GRAPH:
    case RS_SCOPE:
    case RS_KERNEL:
    case RS_UNKNOWN:
    default:
        addBizPartSubGraph();
        if (graphInfo.partId != INVALID_NAVI_PART_ID) {
            return requireGraph;
        } else {
            return INVALID_GRAPH_ID;
        }
    }
}

bool InitResourceGraphBuilder::addSaveToPublish(GraphId from) {
    if (INVALID_GRAPH_ID == from) {
        return true;
    }
    if (_partGraphIds.empty()) {
        return true;
    }
    for (const auto &pair : _partGraphIds) {
        auto to = pair.second;
        if (!_builder.hasSubGraph(from)) {
            return false;
        }
        if (!_builder.hasSubGraph(to)) {
            return false;
        }
        auto fromPort = _builder.node(from, RESOURCE_SAVE_KERNEL).out(RESOURCE_SAVE_KERNEL_OUTPUT);
        _builder.node(to, RESOURCE_PUBLISH_KERNEL).in(RESOURCE_PUBLISH_KERNEL_INPUT).autoNext().from(fromPort);
    }
    return true;
}

InitResourceGraphBuilder::SubGraphInfo InitResourceGraphBuilder::getGraphInfo(
        GraphId graphId) const
{
    auto it = _graphInfoMap.find(graphId);
    if (_graphInfoMap.end() != it) {
        return it->second;
    } else {
        return { RS_UNKNOWN, INVALID_NAVI_PART_ID };
    }
}

void InitResourceGraphBuilder::validate(const ResourceCreator *creator,
                                        const RequireNodeInfo &requireInfo)
{
    assert(creator);
    auto stage = creator->getStage();
    if (stage < RS_RUN_GRAPH_EXTERNAL) {
        auto requireGraphStage = getGraphInfo(requireInfo.graphId).stage;
        if (requireGraphStage < stage) {
            NAVI_KERNEL_LOG(ERROR,
                            "can't depend on resource [%s] stage [%s] in stage "
                            "[%s] graph [%d], depend by resource [%s]",
                            creator->getName().c_str(),
                            ResourceStage_Name(stage).c_str(),
                            ResourceStage_Name(requireGraphStage).c_str(),
                            requireInfo.graphId, requireInfo.node.c_str());
            _hasError = true;
        }
    }
    if (requireInfo.graphId != INVALID_GRAPH_ID) {
        auto requireCreator = _creatorManager->getResourceCreator(requireInfo.node);
        if (requireCreator &&
            requireCreator->isBuildin() &&
            !creator->isBuildin())
        {
            NAVI_KERNEL_LOG(
                ERROR,
                "can't depend on biz module resource [%s] stage [%s] in "
                "buildin resource [%s] stage [%s]",
                creator->getName().c_str(), ResourceStage_Name(stage).c_str(),
                requireCreator->getName().c_str(),
                ResourceStage_Name(requireCreator->getStage()).c_str());
            _hasError = true;
        }
    }
}

} // namespace navi
