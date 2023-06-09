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
#include "navi/builder/SubGraphBuildInfo.h"
#include "navi/builder/NImpl.h"
#include "navi/builder/NImplFake.h"
#include "navi/builder/NImplFork.h"

namespace navi {

SubGraphBuildInfo::SubGraphBuildInfo(GraphBuilder *builder,
                                     SubGraphDef *subGraphDef)
    : _builder(builder)
    , _subGraphDef(subGraphDef)
    , _graphId(subGraphDef->graph_id())
    , _isFork(subGraphDef->location().biz_name() == NAVI_FORK_BIZ)
{
    loadNodeMap();
}

SubGraphBuildInfo::~SubGraphBuildInfo() {
    for (const auto &pair : _nodeMap) {
        delete pair.second;
    }
    for (auto node : _fakeNodeVec) {
        delete node;
    }
}

void SubGraphBuildInfo::location(const std::string &bizName,
                                 NaviPartId partCount,
                                 NaviPartId partId)
{
    initLocation(_subGraphDef->mutable_location(), bizName, partCount, partId, true);
}

void SubGraphBuildInfo::initLocation(LocationDef *location,
                                     const std::string &bizName,
                                     NaviPartId partCount,
                                     NaviPartId partId,
                                     bool enableProbe)
{
    location->set_biz_name(bizName);
    location->mutable_part_info()->set_part_count(partCount);
    location->set_this_part_id(partId);
    location->mutable_gig_info()->set_enable_probe(true);
}

void SubGraphBuildInfo::partIds(const std::vector<NaviPartId> &partIds) {
    auto location = _subGraphDef->mutable_location();
    auto *partInfo = location->mutable_part_info();
    partInfo->clear_part_ids();
    for (const auto &id : partIds) {
        partInfo->add_part_ids(id);
    }
}

void SubGraphBuildInfo::gigProbe(bool enable) {
    auto location = _subGraphDef->mutable_location();
    location->mutable_gig_info()->set_enable_probe(enable);
}

void SubGraphBuildInfo::gigTag(const std::string &tag, multi_call::TagMatchType type) {
    auto location = _subGraphDef->mutable_location();
    auto tagMap = location->mutable_gig_info()->mutable_tags();
    (*tagMap)[tag] = (int32_t)type;
}

void SubGraphBuildInfo::subGraphAttr(const std::string &key, std::string &&value) {
    auto binaryAttrMap = _subGraphDef->mutable_binary_attrs();
    (*binaryAttrMap)[key] = std::move(value);
}

void SubGraphBuildInfo::inlineMode(bool inlineMode) {
    _subGraphDef->mutable_option()->set_inline_mode(inlineMode);
}

void SubGraphBuildInfo::testerMode(bool testerMode) {
    _subGraphDef->mutable_option()->set_tester_mode(testerMode);
}

void SubGraphBuildInfo::loadNodeMap() {
    auto nodeCount = _subGraphDef->nodes_size();
    for (int32_t i = 0; i < nodeCount; i++) {
        auto nodeDef = _subGraphDef->mutable_nodes(i);
        const auto &name = nodeDef->name();
        _nodeDefMap.emplace(name, nodeDef);
    }
}

N SubGraphBuildInfo::addNode(const std::string &name, NodeType nodeType) {
    auto n = addNodeInner(name, nodeType);
    if (nodeType == NT_SPLIT) {
        if (!_isFork) {
            auto t = getTerminatorInputPort();
            t.from(n.out(NODE_FINISH_PORT));
        }
    } else if (nodeType == NT_MERGE) {
        if (_isFork) {
            auto t = getTerminatorInputPort();
            t.from(n.out(NODE_FINISH_PORT));
        }
    }
    return n;
}

P SubGraphBuildInfo::getTerminatorInputPort() {
    if (!_terminatorInput._impl) {
        auto t = addNodeInner(TERMINATOR_NODE_NAME, NT_TERMINATOR);
        t.kernel(TERMINATOR_KERNEL_NAME);
        _terminatorInput = t.in(TERMINATOR_INPUT_PORT);
    }
    return _terminatorInput;
}

N SubGraphBuildInfo::addNodeInner(const std::string &name, NodeType nodeType) {
    auto it = _nodeMap.find(name);
    if (_nodeMap.end() != it) {
        return N(it->second);
    }
    switch (nodeType) {
    case NT_NORMAL:
    case NT_SPLIT:
    case NT_MERGE:
    case NT_RESOURCE:
    case NT_TERMINATOR: {
        NodeDef *nodeDef = nullptr;
        auto it2 = _nodeDefMap.find(name);
        if (_nodeDefMap.end() != it2) {
            nodeDef = it2->second;
        } else {
            nodeDef = _subGraphDef->add_nodes();
            nodeDef->set_name(name);
            nodeDef->set_type(nodeType);
        }
        auto node = new NImpl();
        node->init(_builder, _graphId, nodeDef, nodeType);
        _nodeMap.emplace(name, node);
        return N(node);
    }
    case NT_FORK: {
        auto node = new NImplFork();
        node->init(_builder, _graphId, name, "", nodeType);
        _fakeNodeVec.push_back(node);
        return N(node);
    }
    case NT_FAKE_SPLIT:
    case NT_FAKE_MERGE:
    default: {
        auto node = new NImplFake();
        node->init(_builder, _graphId, name, "", nodeType);
        _fakeNodeVec.push_back(node);
        return N(node);
    }
    }
}

bool SubGraphBuildInfo::hasNode(const std::string &node) const {
    return _nodeMap.find(node) != _nodeMap.end();
}

SubGraphDef *SubGraphBuildInfo::getSubGraphDef() const {
    return _subGraphDef;
}

} // namespace navi
