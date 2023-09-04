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
#ifndef NAVI_GRAPHBUILDER_H
#define NAVI_GRAPHBUILDER_H

#include "autil/legacy/json.h"
#include "navi/common.h"
#include "navi/builder/BorderId.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/builder/GraphDesc.h"
#include <multi_call/common/common.h>
#include <unordered_map>

namespace navi {

class SubGraphBuildInfo;

class GraphBuilder
{
public:
    GraphBuilder(GraphDef *def);
    GraphBuilder(SubGraphDef *subGraphDef);
    ~GraphBuilder();
private:
    GraphBuilder(const GraphBuilder &);
    GraphBuilder &operator=(const GraphBuilder &);
public:
    GraphId newSubGraph(const std::string &bizName);
    ScopeId addScope();
    GraphBuilder &scope(ScopeId scope);
    ScopeId getCurrentScope() const;
    N getScopeTerminator(ScopeId scope = ScopeId());
    GraphBuilder &location(const std::string &bizName, NaviPartId partCount,
                           NaviPartId partId);
    GraphBuilder &partIds(const std::vector<NaviPartId> &partIds);
    GraphBuilder &gigProbe(bool enable);
    GraphBuilder &gigTag(const std::string &tag, multi_call::TagMatchType type);
    GraphBuilder &subGraph(GraphId graphId);
    GraphBuilder &inlineMode(bool inlineMode);
    GraphBuilder &subGraphAttr(const std::string &key, std::string value);
    GraphBuilder &ignoreIsolate(bool ignoreIsolate);
    GraphBuilder &replaceR(const std::string &from, const std::string &to);
    N node(const std::string &name);
    N node(GraphId graphId, const std::string &name);
    bool hasNode(GraphId graphId, const std::string &node) const;
    bool hasNode(const std::string &node) const;
    N getOutputNode(const std::string &name);
    GraphId currentSubGraph() const;
    bool hasSubGraph(GraphId graphId) const;
    std::vector<GraphId> getAllGraphId() const;
    std::vector<ScopeId> getAllScope(GraphId graphId) const;
    bool ok();
private:
    void initFromGraphDef();
    SubGraphBuildInfo *addGraphBuildInfo(SubGraphDef *subGraphDef);
    SubGraphBuildInfo *getGraphBuildInfo(GraphId graphId) const;
    SubGraphDef *getSubGraphDef(GraphId graph) const;
    N nodeInner(GraphId graphId, const std::string &name, NodeType nodeType, int32_t scope = -1);
    N resourceNode(const std::string &name);
    N resourceNode(GraphId graphId, const std::string &name);
    N getParentNode();
    GraphId addRSGraphResourceGraph();
    void addOutputGraph(GraphId graphId, const std::string &bizName);
    BorderDef *getBorderDef(const BorderId &borderId);
    EdgeDef *linkInternal(const GraphId graphId,
                          const std::string &inputNode,
                          const std::string &inputPort,
                          const std::string &outputNode,
                          const std::string &outputPort);
    BorderEdgeDef *linkBorder(const BorderId &borderId,
                              const std::string &inputNode,
                              const std::string &inputPort,
                              const std::string &outputNode,
                              const std::string &outputPort);
    void fillEdgeInfo(EdgeDef *edge,
                      const std::string &inputNode,
                      const std::string &inputPort,
                      const std::string &outputNode,
                      const std::string &outputPort);
    int32_t nextGraphId();
    int32_t nextEdgeId();
private:
    friend class SubGraphBuildInfo;
    friend class PImpl;
    friend class InterEImpl;
    friend class BorderEImpl;
    friend class Graph;
    friend class BizManager;
    friend class InitResourceGraphBuilder;
    friend class RunResourceGraphBuilder;
private:
    GraphDef *_def;
    GraphCounterInfo *_counterInfo;
    GraphId _graphId;
    std::unordered_map<GraphId, SubGraphBuildInfo *> _graphInfoMap;
    std::unordered_map<BorderId, BorderDef *, BorderIdHash> _borderMap;
};

NAVI_TYPEDEF_PTR(GraphBuilder);

}

#endif //NAVI_GRAPHBUILDER_H
