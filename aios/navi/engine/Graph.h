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

#include "navi/engine/GraphBorder.h"
#include "navi/engine/TimeoutChecker.h"
#include "navi/engine/NaviError.h"
#include "navi/proto/GraphDef.pb.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

namespace navi {

enum GraphFinishType {
    GFT_PARENT_FINISH,
    GFT_THIS_FINISH,
    GFT_SUB_FINISH,
};

class SubGraphBase;
class GraphDomainFork;
class Node;

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
    void notifyFinishEc(ErrorCode ec);
    void notifyFinish(const NaviErrorPtr &naviError);
    void notifyFinish(const NaviErrorPtr &error, GraphFinishType type);
    void notifyTimeout();
    void setGraphDomain(GraphId graphId, const GraphDomainPtr &domain);
    void setForkInfo(GraphId graphId, const GraphDomainPtr &domain,
                     Node *forkNode, bool errorAsEof);
    GraphParam *getGraphParam() const;
    void setResourceMap(const ResourceMap *resourceMap);
    void setSubResourceMap(const std::map<GraphId, ResourceMapPtr> &subResourceMaps);
    const std::string &getLocalBizName() const;
    NaviPartId getLocalPartId() const;
    void setErrorAsEof(bool errorAsEof);
    bool isTimeout() const;
    int64_t getRemainTimeMs() const;
    bool updateTimeoutMs(int64_t timeoutMs);
    const TimeoutChecker *getTimeoutChecker() const;
    GraphMetric *getGraphMetric() {
        return _param->graphResult->getGraphMetric();
    }
    Node *getForkNode() const {
        return _forkNode;
    }
    void collectMetric();
private:
    ErrorCode initGraphDef(GraphDef *graphDef);
    ErrorCode rewrite(GraphBuilder &builder, SubGraphDef *subGraphDef);
    RewriteInfoPtr prepareRewriteInfo(SubGraphDef *subGraphDef, NaviPartId partCount, NaviPartId partId);
    ErrorCode addResourceNode(SubGraphDef *subGraphDef, GraphBuilder &builder, const RewriteInfo &rewriteInfo);
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
    bool validateInputResource(const BizPtr &biz) const;
    bool fillInputResource(GraphId graphId, const ResourceMapPtr &resourceMap);
    bool tryInitThisPartIdFromPartIds(SubGraphDef *subGraphDef,
                                      bool &isLocalGraph,
                                      NaviPartId &partCount,
                                      NaviPartId &partId);
    void addSubGraph(SubGraphBase *subGraph);
    ErrorCode initNaviUserResult();
    void initTimeoutChecker();
    bool initTimer();
    void stopTimer();
    bool initBorder();
    void terminateAll(ErrorCode ec);
    SubGraphBorderMapPtr getBorderMap(GraphId graphId) const;
    bool isLocal(const LocationDef &location) const;
    GraphDomainPtr getGraphDomain(GraphId graphId) const;
    GraphDomainFork *getForkDomain() const;
    BizPtr getBiz(const SubGraphDef &subGraphDef);
    BizPtr doGetBiz(const SubGraphDef &subGraphDef);
    ResourceMapPtr getSubResourceMap(GraphId graphId) const;
    void doCollectMetric();
private:
    static const std::string &getBizName(const SubGraphDef &subGraphDef);
    static void initThisPartId(SubGraphDef *subGraphDef);
    static void timeoutCallback(void *data, bool isTimeout);
protected:
    DECLARE_LOGGER();
    GraphParam *_param;
    GraphBorderPtr _border;
    std::unordered_map<GraphId, RewriteInfoPtr> _rewriteInfoMap;
    std::vector<SubGraphBase *> _subGraphs;
    std::set<GraphDomainPtr> _domainSet;
    GraphDomainMapPtr _graphDomainMap;
    std::unordered_map<SubGraphDef *, GraphDomainPtr> _subGraphDomainMap;
    Graph *_parent;
    Node *_forkNode;
    bool _errorAsEof;
    autil::RecursiveThreadMutex _terminateLock;
    std::atomic<bool> _terminated;
    autil::ThreadMutex _metricCollectLock;
    bool _metricCollected; // need_restore
    autil::ThreadMutex _timerLock;
    Timer *_timer;
    TimeoutChecker _timeoutChecker;
    ResourceMap _inputResourceMap;
    std::map<GraphId, ResourceMapPtr> _subResourceMaps;
private:
    std::string _localBizName;
    NaviPartId _localPartId = INVALID_NAVI_PART_ID;
};

}
