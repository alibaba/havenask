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
#pragma once

#include "navi/common.h"
#include "navi/builder/GraphDesc.h"
#include "navi/proto/GraphDef.pb.h"
#include <multi_call/common/common.h>

namespace navi {

class GraphBuilder;
class SubGraphDef;
class NImpl;

class SubGraphBuildInfo {
public:
    SubGraphBuildInfo(GraphBuilder *builder, SubGraphDef *subGraphDef);
    ~SubGraphBuildInfo();
    void location(const std::string &bizName, NaviPartId partCount, NaviPartId partId);
    void partIds(const std::vector<NaviPartId> &partIds);
    void gigProbe(bool enable);
    void gigTag(const std::string &tag, multi_call::TagMatchType type);
    void subGraphAttr(const std::string &key, std::string &&value);
    void inlineMode(bool inlineMode);
    void ignoreIsolate(bool ignoreIsolate);
    void replaceR(const std::string &from, const std::string &to);
    N addNode(const std::string &name, NodeType nodeType, int32_t scope);
    bool hasNode(const std::string &node) const;
    SubGraphDef *getSubGraphDef() const;
    ScopeId addScope();
    ScopeId getCurrentScope() const;
    std::vector<ScopeId> getAllScope() const;
    void setCurrentScope(int32_t index);
    NImplBase *getScopeTerminator(int32_t scopeIndex);
    GraphBuilder *getBuilder() const {
        return _builder;
    }
    GraphId getGraphId() const {
        return _graphId;
    }
    bool isFork() const {
        return _isFork;
    }
    N createScopeTerminator(int32_t scopeIndex);
    P getTerminatorInputPort();
public:
    static void initLocation(LocationDef *location,
                             const std::string &bizName,
                             NaviPartId partCount,
                             NaviPartId partId,
                             bool enableProbe);
private:
    void initScope();
    void loadNodeMap();
    N addNodeInner(const std::string &name, NodeType nodeType, int32_t scope);
    NImpl *addNodeFromDef(NodeDef *def);
    int32_t nextScopeId();
    NImplBase *getExistNode(const std::string &nodeName);
    std::string getScopeTerminatorName(int32_t scopeIndex) const;
    void initScopeBorder(int32_t scopeIndex);
private:
    GraphBuilder *_builder;
    SubGraphDef *_subGraphDef;
    GraphId _graphId;
    bool _isFork;
    std::unordered_map<std::string, NodeDef *> _nodeDefMap;
    std::unordered_map<std::string, NImplBase *> _nodeMap;
    std::vector<NImplBase *> _fakeNodeVec;
    P _terminatorInput;
    std::set<int32_t> _scopeSet;
    ScopeId _currentScope;
};

}
