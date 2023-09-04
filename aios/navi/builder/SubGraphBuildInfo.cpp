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
#include "navi/builder/GraphBuilder.h"

namespace navi {

SubGraphBuildInfo::SubGraphBuildInfo(GraphBuilder *builder,
                                     SubGraphDef *subGraphDef)
    : _builder(builder)
    , _subGraphDef(subGraphDef)
    , _graphId(subGraphDef->graph_id())
    , _isFork(subGraphDef->location().biz_name() == NAVI_FORK_BIZ)
{
    initScope();
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

void SubGraphBuildInfo::ignoreIsolate(bool ignoreIsolate) {
    _subGraphDef->mutable_option()->set_ignore_isolate(ignoreIsolate);
}

void SubGraphBuildInfo::replaceR(const std::string &from, const std::string &to) {
    auto replaceInfo = _subGraphDef->add_replace_infos();
    replaceInfo->set_from(from);
    replaceInfo->set_to(to);
}

void SubGraphBuildInfo::initScope() {
    auto count = _subGraphDef->scopes_size();
    if (0 == count) {
        auto id = nextScopeId();
        _currentScope = ScopeId(_graphId, id);
        _scopeSet.insert(id);
        _subGraphDef->add_scopes(id);
    } else {
        for (int32_t i = 0; i < count; i++) {
            auto id = _subGraphDef->scopes(i);
            if (i == 0) {
                _currentScope = ScopeId(_graphId, id);
            }
            _scopeSet.insert(id);
        }
    }
}

void SubGraphBuildInfo::loadNodeMap() {
    auto nodeCount = _subGraphDef->nodes_size();
    for (int32_t i = 0; i < nodeCount; i++) {
        auto nodeDef = _subGraphDef->mutable_nodes(i);
        const auto &name = nodeDef->name();
        _nodeDefMap.emplace(name, nodeDef);
    }
}

N SubGraphBuildInfo::addNode(const std::string &name, NodeType nodeType, int32_t scope) {
    auto n = addNodeInner(name, nodeType, scope);
    if (nodeType == NT_SPLIT || nodeType == NT_SCOPE_TERMINATOR_SPLIT) {
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
        auto t = addNodeInner(TERMINATOR_NODE_NAME, NT_TERMINATOR, 0);
        t.kernel(TERMINATOR_KERNEL_NAME);
        _terminatorInput = t.in(TERMINATOR_INPUT_PORT);
    }
    return _terminatorInput;
}

NImplBase *SubGraphBuildInfo::getScopeTerminator(int32_t scopeIndex) {
    auto scopeTerminatorName = getScopeTerminatorName(scopeIndex);
    return getExistNode(scopeTerminatorName);
}

N SubGraphBuildInfo::createScopeTerminator(int32_t scopeIndex) {
    if (scopeIndex < 0) {
        scopeIndex = _currentScope.scopeIndex;
    }
    {
        auto it = _scopeSet.find(scopeIndex);
        if (it == _scopeSet.end()) {
            auto msg = "can't get scope terminator, scope not exist in subGraph, subGraphId: " +
                       autil::StringUtil::toString(_graphId) + ", scopeId: " + autil::StringUtil::toString(scopeIndex);
            throw autil::legacy::ExceptionBase(msg);
        }
    }
    auto scopeTerminatorName = getScopeTerminatorName(scopeIndex);
    auto existNode = getExistNode(scopeTerminatorName);
    if (existNode) {
        return N(existNode);
    }
    auto n = _builder->nodeInner(_graphId, scopeTerminatorName, NT_SCOPE_TERMINATOR, scopeIndex);
    auto graphTerminator = getTerminatorInputPort();
    graphTerminator.from(n.out(NODE_FINISH_PORT));
    initScopeBorder(scopeIndex);
    return n;
}

NImplBase *SubGraphBuildInfo::getExistNode(const std::string &nodeName) {
    {
        auto it = _nodeMap.find(nodeName);
        if (_nodeMap.end() != it) {
            return it->second;
        }
    }
    {
        auto it = _nodeDefMap.find(nodeName);
        if (_nodeDefMap.end() != it) {
            auto def = it->second;
            return addNodeFromDef(def);
        }
    }
    return nullptr;
}

void SubGraphBuildInfo::initScopeBorder(int32_t scopeIndex) {
    for (const auto &pair : _nodeDefMap) {
        const auto &name = pair.first;
        auto def = pair.second;
        if (scopeIndex != def->scope()) {
            continue;
        }
        if (!def->is_scope_border()) {
            continue;
        }
        auto n = addNodeInner(name, def->type(), scopeIndex);
        n._impl->setScopeBorder();
    }
}

std::string SubGraphBuildInfo::getScopeTerminatorName(int32_t scopeIndex) const {
    return SCOPE_TERMINATOR_NODE_NAME_PREFIX + std::to_string(getGraphId()) + ":" + std::to_string(scopeIndex);
}

N SubGraphBuildInfo::addNodeInner(const std::string &name, NodeType nodeType, int32_t scope) {
    auto it = _nodeMap.find(name);
    if (_nodeMap.end() != it) {
        return N(it->second);
    }
    switch (nodeType) {
    case NT_NORMAL:
    case NT_SPLIT:
    case NT_MERGE:
    case NT_RESOURCE:
    case NT_TERMINATOR:
    case NT_SCOPE_TERMINATOR:
    case NT_SCOPE_TERMINATOR_SPLIT: {
        NodeDef *nodeDef = nullptr;
        auto it2 = _nodeDefMap.find(name);
        if (_nodeDefMap.end() != it2) {
            nodeDef = it2->second;
        } else {
            nodeDef = _subGraphDef->add_nodes();
            nodeDef->set_name(name);
            nodeDef->set_type(nodeType);
            int32_t realScope = scope;
            if (realScope < 0) {
                realScope = getCurrentScope().scopeIndex;
            }
            nodeDef->set_scope(realScope);
            _nodeDefMap.emplace(name, nodeDef);
        }
        return N(addNodeFromDef(nodeDef));
    }
    case NT_FORK: {
        auto node = new NImplFork();
        node->init(_builder, _graphId, name, "", nodeType);
        _nodeMap.emplace(name, node);
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

NImpl *SubGraphBuildInfo::addNodeFromDef(NodeDef *def) {
    auto node = new NImpl();
    node->init(def, this);
    _nodeMap.emplace(def->name(), node);
    return node;
}

bool SubGraphBuildInfo::hasNode(const std::string &node) const {
    return _nodeMap.find(node) != _nodeMap.end();
}

SubGraphDef *SubGraphBuildInfo::getSubGraphDef() const {
    return _subGraphDef;
}

ScopeId SubGraphBuildInfo::addScope() {
    auto id = nextScopeId();
    _currentScope.scopeIndex = id;
    auto it = _scopeSet.find(id);
    if (_scopeSet.end() == it) {
        _scopeSet.insert(id);
        _subGraphDef->add_scopes(id);
    }
    return _currentScope;
}

ScopeId SubGraphBuildInfo::getCurrentScope() const {
    return _currentScope;
}

void SubGraphBuildInfo::setCurrentScope(int32_t index) {
    auto it = _scopeSet.find(index);
    if (it == _scopeSet.end()) {
        auto msg = "scope not exist in subGraph, subGraphId: " + autil::StringUtil::toString(_graphId) +
                   ", scopeId: " + autil::StringUtil::toString(index);
        throw autil::legacy::ExceptionBase(msg);
    }
    _currentScope.scopeIndex = index;
}

std::vector<ScopeId> SubGraphBuildInfo::getAllScope() const {
    std::vector<ScopeId> ret;
    for (auto index : _scopeSet) {
        ret.push_back(ScopeId(_graphId, index));
    }
    return ret;
}

int32_t SubGraphBuildInfo::nextScopeId() {
    auto id = _subGraphDef->next_scope_id();
    _subGraphDef->set_next_scope_id(id + 1);
    return id;
}

} // namespace navi
