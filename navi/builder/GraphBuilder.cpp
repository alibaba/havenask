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
#include "navi/builder/GraphBuilder.h"
#include "autil/legacy/jsonizable.h"
#include "navi/builder/SubGraphBuildInfo.h"

using namespace std;

namespace navi {

GraphBuilder::GraphBuilder(GraphDef *def)
    : _def(def)
    , _counterInfo(nullptr)
    , _graphId(INVALID_GRAPH_ID)
{
    _def->set_version(NAVI_VERSION);
    initFromGraphDef();
}

GraphBuilder::GraphBuilder(SubGraphDef *subGraphDef)
    : _def(nullptr)
    , _counterInfo(nullptr)
{
    _graphId = subGraphDef->graph_id();
    addGraphBuildInfo(subGraphDef);
}

GraphBuilder::~GraphBuilder() {
    for (const auto &pair : _graphInfoMap) {
        delete pair.second;
    }
}

void GraphBuilder::initFromGraphDef() {
    _counterInfo = _def->mutable_counter_info();
    auto graphCount = _def->sub_graphs_size();
    for (int i = 0; i < graphCount; i++) {
        addGraphBuildInfo(_def->mutable_sub_graphs(i));
    }
}

GraphId GraphBuilder::newSubGraph(const std::string &bizName) {
    if (!_def) {
        return INVALID_GRAPH_ID;
    }
    auto subGraphDef = _def->add_sub_graphs();
    auto graphId = nextGraphId();
    subGraphDef->set_graph_id(graphId);
    addGraphBuildInfo(subGraphDef);
    subGraph(graphId);
    location(bizName, INVALID_NAVI_PART_COUNT, INVALID_NAVI_PART_ID);
    return graphId;
}

GraphBuilder &GraphBuilder::location(const std::string &bizName,
                                     NaviPartId partCount,
                                     NaviPartId partId)
{
    auto graphBuildInfo = getGraphBuildInfo(_graphId);
    assert(graphBuildInfo);
    graphBuildInfo->location(bizName, partCount, partId);
    return *this;
}

GraphBuilder &GraphBuilder::partIds(const std::vector<NaviPartId> &partIds)
{
    auto graphBuildInfo = getGraphBuildInfo(_graphId);
    assert(graphBuildInfo);
    graphBuildInfo->partIds(partIds);
    return *this;
}

GraphBuilder &GraphBuilder::gigProbe(bool enable) {
    auto graphBuildInfo = getGraphBuildInfo(_graphId);
    assert(graphBuildInfo);
    graphBuildInfo->gigProbe(enable);
    return *this;
}

GraphBuilder &GraphBuilder::gigTag(const std::string &tag, multi_call::TagMatchType type) {
    auto graphBuildInfo = getGraphBuildInfo(_graphId);
    assert(graphBuildInfo);
    graphBuildInfo->gigTag(tag, type);
    return *this;
}

GraphBuilder &GraphBuilder::inlineMode(bool inlineMode) {
    auto graphBuildInfo = getGraphBuildInfo(_graphId);
    assert(graphBuildInfo);
    graphBuildInfo->inlineMode(inlineMode);
    return *this;
}

GraphBuilder &GraphBuilder::subGraphAttr(const std::string &key, std::string value)
{
    auto graphBuildInfo = getGraphBuildInfo(_graphId);
    assert(graphBuildInfo);
    graphBuildInfo->subGraphAttr(key, std::move(value));
    return *this;
}

GraphBuilder &GraphBuilder::testerMode(bool testerMode) {
    auto graphBuildInfo = getGraphBuildInfo(_graphId);
    assert(graphBuildInfo);
    graphBuildInfo->testerMode(testerMode);
    return *this;
}

SubGraphBuildInfo *GraphBuilder::addGraphBuildInfo(SubGraphDef *subGraphDef) {
    auto info = new SubGraphBuildInfo(this, subGraphDef);
    _graphInfoMap.emplace(subGraphDef->graph_id(), info);
    return info;
}

SubGraphBuildInfo *GraphBuilder::getGraphBuildInfo(GraphId graphId) {
    auto it = _graphInfoMap.find(graphId);
    if (_graphInfoMap.end() != it) {
        return it->second;
    } else {
        return nullptr;
    }
}

GraphBuilder &GraphBuilder::subGraph(GraphId graphId) {
    if (_graphInfoMap.end() == _graphInfoMap.find(graphId)) {
        auto msg = "sub graph id [" + autil::StringUtil::toString(graphId) + "] not exist, call newSubGraph first";
        throw autil::legacy::ExceptionBase(msg);
    }
    if (_graphId != graphId) {
        _graphId = graphId;
    }
    return *this;
}

N GraphBuilder::node(const std::string &name) {
    return nodeInner(_graphId, name, NT_NORMAL);
}

N GraphBuilder::node(GraphId graphId, const std::string &name) {
    if (_graphInfoMap.end() == _graphInfoMap.find(graphId)) {
        auto msg = "sub graph id [" + autil::StringUtil::toString(graphId) + "] not exist, call newSubGraph first";
        throw autil::legacy::ExceptionBase(msg);
    }
    return nodeInner(graphId, name, NT_NORMAL);
}

N GraphBuilder::nodeInner(GraphId graphId, const std::string &name, NodeType nodeType) {
    auto graphBuildInfo = getGraphBuildInfo(graphId);
    assert(graphBuildInfo);
    return graphBuildInfo->addNode(name, nodeType);
}

N GraphBuilder::resourceNode(const std::string &name) {
    return nodeInner(_graphId, name, NT_RESOURCE);
}

N GraphBuilder::getParentNode() {
    const auto parentGraph = PARENT_GRAPH_ID;
    addOutputGraph(parentGraph, NAVI_FORK_BIZ);
    return nodeInner(parentGraph, NAVI_FORK_NODE, NT_FORK);
}

N GraphBuilder::getOutputNode(const std::string &name) {
    const auto userGraph = USER_GRAPH_ID;
    addOutputGraph(userGraph, NAVI_USER_GRAPH_BIZ);
    return nodeInner(userGraph, name, NT_NORMAL);
}

EdgeDef *GraphBuilder::linkInternal(const GraphId graphId,
                                    const std::string &inputNode,
                                    const std::string &inputPort,
                                    const std::string &outputNode,
                                    const std::string &outputPort)
{
    auto inputGraphDef = getSubGraphDef(graphId);
    assert(inputGraphDef);
    auto edge = inputGraphDef->add_edges();
    fillEdgeInfo(edge, inputNode, inputPort, outputNode, outputPort);
    return edge;
}

BorderEdgeDef *GraphBuilder::linkBorder(const BorderId &borderId,
                                        const std::string &inputNode,
                                        const std::string &inputPort,
                                        const std::string &outputNode,
                                        const std::string &outputPort)
{
    auto borderDef = getBorderDef(borderId);
    if (!borderDef) {
        return nullptr;
    }
    auto borderEdge = borderDef->add_edges();
    borderEdge->set_edge_id(nextEdgeId());
    auto edge = borderEdge->mutable_edge();
    fillEdgeInfo(edge, inputNode, inputPort, outputNode, outputPort);
    return borderEdge;
}

SubGraphDef *GraphBuilder::getSubGraphDef(GraphId graph) const {
    auto it = _graphInfoMap.find(graph);
    if (_graphInfoMap.end() != it) {
        return it->second->getSubGraphDef();
    } else {
        return nullptr;
    }
}

bool GraphBuilder::hasSubGraph(GraphId graphId) const {
    return (getSubGraphDef(graphId) != nullptr);
}

BorderDef *GraphBuilder::getBorderDef(const BorderId &borderId) {
    auto it = _borderMap.find(borderId);
    if (_borderMap.end() != it) {
        return it->second;
    } else {
        auto graphDef = getSubGraphDef(borderId.id);
        if (!graphDef) {
            return nullptr;
        }
        auto borderDef = graphDef->add_borders();
        borderDef->set_io_type((int32_t)borderId.ioType);
        auto peerInfo = borderDef->mutable_peer();
        peerInfo->set_graph_id(borderId.peer);
        auto *partInfo = peerInfo->mutable_part_info();
        partInfo->set_part_count(INVALID_NAVI_PART_COUNT);
        _borderMap.emplace(borderId, borderDef);
        return borderDef;
    }
}

void GraphBuilder::addOutputGraph(GraphId graphId,
                                  const std::string &bizName)
{
    auto outputGraph = getSubGraphDef(graphId);
    if (!outputGraph) {
        outputGraph = _def->add_sub_graphs();
        outputGraph->set_graph_id(graphId);
        auto location = outputGraph->mutable_location();
        SubGraphBuildInfo::initLocation(location, bizName,
                INVALID_NAVI_PART_COUNT, INVALID_NAVI_PART_ID, true);
        addGraphBuildInfo(outputGraph);
    }
}

void GraphBuilder::fillEdgeInfo(EdgeDef *edge,
                                const std::string &inputNode,
                                const std::string &inputPort,
                                const std::string &outputNode,
                                const std::string &outputPort)
{
    auto input = edge->mutable_input();
    input->set_node_name(inputNode);
    input->set_port_name(inputPort);
    auto output = edge->mutable_output();
    output->set_node_name(outputNode);
    output->set_port_name(outputPort);
}

bool GraphBuilder::hasNode(const std::string &node) const {
    return hasNode(_graphId, node);
}

bool GraphBuilder::hasNode(GraphId graphId, const std::string &node) const {
    auto it = _graphInfoMap.find(graphId);
    if (_graphInfoMap.end() != it) {
        auto graphBuildInfo = it->second;
        return graphBuildInfo->hasNode(node);
    } else {
        return false;
    }
}

GraphId GraphBuilder::currentSubGraph() const {
    return _graphId;
}

bool GraphBuilder::ok() {
    return true;
}

int32_t GraphBuilder::nextGraphId() {
    if (!_counterInfo) {
        auto msg = "can't add new sub graph in single subGraph builder, subGraphId: " +
                   autil::StringUtil::toString(_graphId);
        throw autil::legacy::ExceptionBase(msg);
    }
    auto id = _counterInfo->next_graph_id();
    _counterInfo->set_next_graph_id(id + 1);
    return id;
}

int32_t GraphBuilder::nextEdgeId() {
    if (!_counterInfo) {
        auto msg = "can't add cross graph edge in single subGraph builder, subGraphId: " +
                   autil::StringUtil::toString(_graphId);
        throw autil::legacy::ExceptionBase(msg);
    }
    auto id = _counterInfo->next_edge_id();
    _counterInfo->set_next_edge_id(id + 1);
    return id;
}

}
