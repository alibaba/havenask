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
#ifndef NAVI_LOCALSUBGRAPH_H
#define NAVI_LOCALSUBGRAPH_H

#include "navi/builder/GraphBuilder.h"
#include "navi/engine/Biz.h"
#include "navi/engine/Data.h"
#include "navi/engine/GraphParam.h"
#include "navi/engine/Port.h"
#include "navi/engine/SubGraphBase.h"
#include "navi/engine/SubGraphBorder.h"
#include "navi/log/NaviLogger.h"
#include <unordered_map>
#include <vector>

namespace navi {

class NaviWorkerBase;
class Node;
class Edge;
class Resource;

struct ScopeInfo {
    ErrorCode ec = EC_NONE;
    std::atomic<bool> terminated = false;
    Node *terminator = nullptr;
    std::vector<Node *> nodes;
};

class LocalSubGraph : public SubGraphBase
{
public:
    LocalSubGraph(SubGraphDef *subGraphDef,
                  GraphParam *param,
                  const GraphDomainPtr &domain,
                  const SubGraphBorderMapPtr &borderMap,
                  Graph *graph,
                  const RewriteInfoPtr &rewriteInfo);
    ~LocalSubGraph();
private:
    LocalSubGraph(const LocalSubGraph &);
    LocalSubGraph& operator=(const LocalSubGraph &);
public:
    ErrorCode init() override;
    ErrorCode run() override;
    void terminate(ErrorCode ec) override;
public:
    Node *getNode(const std::string &nodeName) const;
    bool schedule(Node *node);
    void notifyFinish(Node *node, ErrorCode ec);
    NaviWorkerBase *getWorker() const;
    const BizPtr &getBiz() const;
    bool bindPortDataType(Node *node, Port *port, IoType ioType);
    bool saveResource(const ResourceMap &resourceMap);
    bool publishResource();
    void collectResourceMap(MultiLayerResourceMap &multiLayerResourceMap) const;
    ResourceStage getResourceStage(const std::string &name) const;
    const std::map<std::string, bool> *getResourceDependResourceMap(const std::string &name) const;
    const std::unordered_set<std::string> *getResourceReplacerSet(const std::string &name) const;
    ErrorCode createResource(const std::string &name,
                             const MultiLayerResourceMap &multiLayerResourceMap,
                             const ProduceInfo &produceInfo,
                             NaviConfigContext *ctx,
                             bool checkReuse,
                             Node *requireKernelNode,
                             ResourcePtr &resource) const;
    ResourcePtr buildKernelResourceRecur(const std::string &name,
                                         KernelConfigContext *ctx,
                                         const ResourceMap &nodeResourceMap,
                                         Node *requireKernelNode,
                                         ResourceMap &inputResourceMap);
    bool terminateScope(Node *node, int32_t scopeIndex, ErrorCode ec);
    ErrorCode getScopeErrorCode(int32_t scopeIndex) const;
    const ReplaceInfoMap &getReplaceInfoMap() const {
        return _rewriteInfo->replaceInfoMap;
    }

public:
    TestMode getTestMode() const;

private:
    bool doInit();
    void initSubGraphOption();
    bool addDependResourceRecur(const std::string &resource, bool require,
                                GraphBuilder &builder);
    bool initPortDataType();
    bool setPortType(Port *port, IoType ioType);
    std::string getNodePortDataTypeStr(Node *node, const std::string &portName,
                                       const std::string &peerPortName,
                                       IoType ioType) const;
    bool createNodes();
    void addToScopeInfoMap(Node *node);
    bool bindPortToNode();
    bool createEdges();
    bool bindForkDomain();
    bool isForkDomain() const;
    Node *createNode(const NodeDef &nodeDef);
    bool createEdge(const EdgeDef &edgeDef);
    bool checkGraph() const;
    bool checkNode() const;
    bool checkEdge() const;
    void finishPendingOutput();
    bool postInit();
    void doSchedule();
    ErrorCode flushEdgeOverride();
    bool inlineCompute(NaviWorkerBase *worker, Node *node);
private:
    BizPtr _biz;
    RewriteInfoPtr _rewriteInfo;
    std::unordered_map<std::string, Node *> _nodeMap;
    std::vector<Node *> _nodeVec;
    std::unordered_map<std::string, Edge *> _edges;
    IndexType _finishPortIndex;
    bool _frozen;
    bool _inlineMode;
    std::unordered_map<int32_t, ScopeInfo> _scopeInfoMap;
};

NAVI_TYPEDEF_PTR(LocalSubGraph);

}

#endif //NAVI_LOCALSUBGRAPH_H
