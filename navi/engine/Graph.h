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
#ifndef NAVI_GRAPH_H
#define NAVI_GRAPH_H

#include "navi/engine/GraphBorder.h"
#include "navi/proto/GraphDef.pb.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

namespace navi {

enum GraphFinishType {
    GFT_PARENT_FINISH,
    GFT_THIS_FINISH,
    GFT_SUB_FINISH,
};

class SubGraphBase;

class Graph
{
public:
    Graph(GraphParam *param, Graph *parent);
    ~Graph();
private:
    Graph(const Graph &);
    Graph &operator=(const Graph &);
public:
    ErrorCode init(GraphDef *graphDef);
    ErrorCode run();
    void notifyFinish(ErrorCode ec);
    void notifyFinish(ErrorCode ec, GraphFinishType type);
    void setGraphDomain(GraphId graphId, const GraphDomainPtr &domain);
    GraphParam *getGraphParam() const;
    bool isForkGraph() const {
        return _parent != nullptr;
    }
    void setResourceMap(const ResourceMap *resourceMap);
    const std::string &getLocalBizName() const;
    NaviPartId getLocalPartId() const;
    void setOverrideData(const std::vector<OverrideData> &overrideData);
    const std::vector<OverrideData> &getOverrideData() const;
private:
    bool preInitGraphDomains(GraphDef *graphDef);
    bool postInitGraphDomains();
    bool validOverrideData() const;
    bool runGraphDomains();
    GraphDomainPtr createGraphDomain(GraphId graphId, const LocationDef &location);
    GraphDomainPtr createUserDomain(const std::string &bizName);
    GraphDomainPtr createLocalDomain(const std::string &bizName);
    GraphDomainPtr createRemoteDomain(const std::string &bizName);
    void bindGraphDomain(const GraphDomainPtr &domain,
                         SubGraphDef *subGraphDef);
    ErrorCode createSubGraphs(GraphDef *graphDef);
    SubGraphBase *createSubGraph(SubGraphDef *subGraphDef,
                                 const GraphDomainPtr &domain);
    bool fillInputResource(const ResourceMapPtr &resourceMap);
    void addSubGraph(SubGraphBase *subGraph);
    ErrorCode initNaviResult();
    void fillResult();
    bool initBorder();
    void terminateAll(ErrorCode ec);
    SubGraphBorderMapPtr getBorderMap(GraphId graphId) const;
    bool isLocal(const LocationDef &location) const;
    GraphDomainPtr getGraphDomain(GraphId graphId) const;
    BizPtr getBiz(const GraphDomainPtr &domain, const SubGraphDef &subGraphDef);
    BizPtr getBiz(const SubGraphDef &subGraphDef);
    ResourceMapPtr getResourceMap(const SubGraphDef &subGraphDef);
private:
    static const std::string &getBizName(const SubGraphDef &subGraphDef);
    static void initThisPartId(SubGraphDef *subGraphDef);
protected:
    DECLARE_LOGGER();
    PoolPtr _pool;
    GraphParam *_param;
    GraphBorderPtr _border;
    std::vector<SubGraphBase *> _subGraphs;
    std::set<GraphDomainPtr> _domainSet;
    GraphDomainMapPtr _graphDomainMap;
    std::unordered_map<SubGraphDef *, GraphDomainPtr> _subGraphDomainMap;
    Graph *_parent;
    autil::RecursiveThreadMutex _terminateLock;
    std::atomic<bool> _terminated;
    ResourceMap _inputResourceMap;
    std::vector<OverrideData> _overrideData;
private:
    std::string _localBizName;
    NaviPartId _localPartId = INVALID_NAVI_PART_ID;
};

}

#endif //NAVI_GRAPH_H
