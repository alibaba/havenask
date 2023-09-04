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
#include "navi/engine/Graph.h"

#include "aios/network/gig/multi_call/common/common.h"
#include "navi/engine/GraphDomainClient.h"
#include "navi/engine/GraphDomainFork.h"
#include "navi/engine/GraphDomainLocal.h"
#include "navi/engine/GraphDomainUser.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/RemoteSubGraphBase.h"
#include "navi/log/TraceAppender.h"
#include "navi/resource/TimeoutCheckerR.h"
#include "navi/resource/QuerySessionR.h"
#include "navi/resource/NaviResultR.h"

#include "navi/ops/ResourceKernelDef.h"
#include "navi/util/CommonUtil.h"

using namespace multi_call;

namespace navi {

class TimeoutItem : public NaviNoDropWorkerItem
{
public:
    TimeoutItem(NaviWorkerBase *worker, Graph *graph)
        : NaviNoDropWorkerItem(worker)
        , _graph(graph)
    {
    }
public:
    void doProcess() override {
        _graph->notifyTimeout();
        _worker->decItemCount();
    }
private:
    Graph *_graph;
};

Graph::Graph(GraphParam *param, Graph *parent)
    : _logger(this, "graph", param->logger)
    , _param(param)
    , _graphDomainMap(new GraphDomainMap())
    , _parent(parent)
    , _errorAsEof(false)
    , _terminated(false)
    , _timer(nullptr)
{
}

Graph::~Graph() {
    for (auto subGraph : _subGraphs) {
        delete subGraph;
    }
    delete _param;
}

ErrorCode Graph::init(GraphDef *graphDef) {
    NAVI_LOG(SCHEDULE1, "graph def [%s]", graphDef->DebugString().c_str());
    autil::ScopedLock lock(_terminateLock);
    NaviLoggerScope scope(_logger);
    auto ec = initGraphDef(graphDef);
    if (EC_NONE != ec) {
        return ec;
    }
    if (!preInitGraphDomains(graphDef)) {
        return EC_INIT_GRAPH;
    }
    if (!initBorder()) {
        return EC_INIT_GRAPH;
    }
    if (!postInitGraphDomains()) {
        return EC_INIT_GRAPH;
    }
    ec = createSubGraphs(graphDef);
    if (EC_NONE != ec) {
        return ec;
    }
    NAVI_LOG(SCHEDULE1,
             "after init graph def [%s], graph border [%s]",
             graphDef->DebugString().c_str(),
             autil::StringUtil::toString(*_border).c_str());

    return initNaviResult();
}

ErrorCode Graph::initGraphDef(GraphDef *graphDef) {
    GraphBuilder builder(graphDef);
    auto subCount = graphDef->sub_graphs_size();
    NAVI_LOG(DEBUG, "sub graph count [%d]", subCount);
    for (int32_t i = 0; i < subCount; i++) {
        auto subGraphDef = graphDef->mutable_sub_graphs(i);
        NAVI_LOG(SCHEDULE2, "sub graph before rewrite: %s", subGraphDef->DebugString().c_str());
        auto ec = rewrite(builder, subGraphDef);
        if (EC_NONE != ec) {
            return ec;
        }
        NAVI_LOG(SCHEDULE2, "sub graph after rewrite: %s", subGraphDef->DebugString().c_str());
    }
    auto resourceGraph = builder.getSubGraphDef(RS_GRAPH_RESOURCE_GRAPH_ID);
    if (resourceGraph) {
        if (!prepareRewriteInfo(resourceGraph, 1, 0)) {
            return EC_REWRITE_GRAPH;
        }
    }
    return EC_NONE;
}

ErrorCode Graph::rewrite(GraphBuilder &builder, SubGraphDef *subGraphDef) {
    bool isLocalGraph = false;
    NaviPartId partCount = INVALID_NAVI_PART_COUNT;
    NaviPartId partId = INVALID_NAVI_PART_ID;
    if (!tryInitThisPartIdFromPartIds(subGraphDef, isLocalGraph, partCount, partId)) {
        return EC_REWRITE_GRAPH;
    }
    if (!isLocalGraph) {
        NAVI_LOG(DEBUG, "not local sub graph [%d], rewrite skipped", subGraphDef->graph_id());
        return EC_NONE;
    }
    auto rewriteInfo = prepareRewriteInfo(subGraphDef, partCount, partId);
    if (!rewriteInfo) {
        return EC_REWRITE_GRAPH;
    }
    auto ec = addResourceNode(subGraphDef, builder, *rewriteInfo);
    if (EC_NONE != ec) {
        return ec;
    }
    return EC_NONE;;
}

RewriteInfoPtr Graph::prepareRewriteInfo(SubGraphDef *subGraphDef, NaviPartId partCount, NaviPartId partId) {
    auto graphId = subGraphDef->graph_id();
    auto biz = getBiz(*subGraphDef);
    if (!biz) {
        NAVI_LOG(ERROR, "get biz failed, subGraph [%d]", graphId);
        return nullptr;
    }
    auto resourceMap = std::make_shared<ResourceMap>();
    if (!validateInputResource(biz)) {
        return nullptr;
    }
    if (!fillInputResource(graphId, resourceMap)) {
        return nullptr;
    }
    auto rewriteInfo = std::make_shared<RewriteInfo>();
    rewriteInfo->biz = biz;
    rewriteInfo->resourceMap = resourceMap;
    rewriteInfo->multiLayerResourceMap.addResourceMap(resourceMap.get());
    if (!biz->collectResourceMap(partCount, partId, rewriteInfo->multiLayerResourceMap)) {
        NAVI_LOG(ERROR, "collectResourceMap failed for sub graph [%d]", subGraphDef->graph_id());
        return nullptr;
    }
    rewriteInfo->replaceInfoMap.init(subGraphDef);
    _rewriteInfoMap[subGraphDef->graph_id()] = rewriteInfo;
    return rewriteInfo;
}

ErrorCode Graph::addResourceNode(SubGraphDef *subGraphDef, GraphBuilder &builder, const RewriteInfo &rewriteInfo) {
    try {
        auto graphId = subGraphDef->graph_id();
        const auto &multiLayerResourceMap = rewriteInfo.multiLayerResourceMap;
        const auto &resourceReplaceMap = rewriteInfo.replaceInfoMap.getReplaceFrom2To();
        const auto &biz = rewriteInfo.biz;
        const auto &subGraph = *subGraphDef;
        auto nodeCount = subGraph.nodes_size();
        builder.subGraph(graphId);
        for (auto i = 0; i < nodeCount; i++) {
            const auto &node = subGraph.nodes(i);
            const auto &nodeName = node.name();
            const auto &kernelName = node.kernel_name();
            auto kernelCreator = biz->getKernelCreator(kernelName);
            if (!kernelCreator) {
                if (kernelName.empty()) {
                    NAVI_LOG(ERROR, "kernel creator not found, kernel name not set, node [%s]", nodeName.c_str());
                } else {
                    NAVI_LOG(ERROR,
                             "kernel creator not found for kernel [%s] node [%s]",
                             kernelName.c_str(),
                             nodeName.c_str());
                }
                return EC_CREATE_KERNEL;
            }
            const auto &dependResourceMap = kernelCreator->getDependResources();
            auto kernelResourceInputPort = builder.node(nodeName).in(NODE_RESOURCE_INPUT_PORT);
            for (const auto &pair : dependResourceMap) {
                const auto &resource = pair.first;
                NAVI_LOG(
                    SCHEDULE2, "add depend resource [%s], depend by kernel [%s]", resource.c_str(), kernelName.c_str());
                auto n = biz->addResourceGraph(
                    resource, multiLayerResourceMap, node.scope(), nodeName, resourceReplaceMap, builder);
                n.out(RESOURCE_CREATE_OUTPUT).to(kernelResourceInputPort.autoNext());
            }
        }
        return EC_NONE;
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_LOG(ERROR, "add resource graph failed, msg [%s]", e.GetMessage().c_str());
        return EC_REWRITE_GRAPH;
    }
}

bool Graph::preInitGraphDomains(GraphDef *graphDef) {
    auto counterInfo = graphDef->mutable_counter_info();
    auto &bizDomainMap = *_graphDomainMap;
    auto subCount = graphDef->sub_graphs_size();
    for (int32_t i = 0; i < subCount; i++) {
        auto subGraphDef = graphDef->mutable_sub_graphs(i);
        const auto &location = subGraphDef->location();
        auto graphId = subGraphDef->graph_id();
        GraphDomainPtr domain;
        auto it = bizDomainMap.find(graphId);
        if (bizDomainMap.end() != it) {
            domain = it->second;
        } else {
            domain = createGraphDomain(graphId, location);
            domain->setCounterInfo(counterInfo);
            bizDomainMap.emplace(graphId, domain);
        }
        bindGraphDomain(domain, subGraphDef);
        _domainSet.insert(domain);
    }
    for (const auto &domain : _domainSet) {
        if (!domain->preInit()) {
            NAVI_LOG(ERROR, "domain preInit failed");
            return false;
        }
    }
    return true;
}

bool Graph::postInitGraphDomains() {
    const auto &bizDomainMap = *_graphDomainMap;
    for (const auto &pair : bizDomainMap) {
        if (!pair.second->postInit()) {
            return false;
        }
        NAVI_LOG(SCHEDULE1, "end post init domain [%s]",
                 autil::StringUtil::toString(*pair.second).c_str());
    }
    return true;
}

bool Graph::runGraphDomains() {
    const auto &bizDomainMap = *_graphDomainMap;
    for (const auto &pair : bizDomainMap) {
        if (!pair.second->run()) {
            return false;
        }
    }
    return true;
}

GraphDomainPtr Graph::createGraphDomain(GraphId graphId, const LocationDef &location) {
    auto domain = getGraphDomain(graphId);
    if (domain) {
        return domain;
    }
    const auto &bizName = location.biz_name();
    if (NAVI_USER_GRAPH_BIZ == bizName) {
        domain = createUserDomain(bizName);
    } else if (_rewriteInfoMap.end() != _rewriteInfoMap.find(graphId)) {
        domain = createLocalDomain(bizName);
    } else {
        domain = createRemoteDomain(bizName);
    }
    return domain;
}

GraphDomainPtr Graph::createUserDomain(const std::string &bizName) {
    return GraphDomainPtr(new GraphDomainUser(this));
}

GraphDomainPtr Graph::createLocalDomain(const std::string &bizName) {
    return GraphDomainPtr(new GraphDomainLocal(this));
}

GraphDomainPtr Graph::createRemoteDomain(const std::string &bizName) {
    return GraphDomainPtr(new GraphDomainClient(this, bizName));
}

void Graph::bindGraphDomain(const GraphDomainPtr &domain,
                            SubGraphDef *subGraphDef)
{
    domain->setSubGraph(subGraphDef);
    _subGraphDomainMap.emplace(subGraphDef, domain);
}

ErrorCode Graph::createSubGraphs(GraphDef *graphDef) {
    auto subCount = graphDef->sub_graphs_size();
    for (int32_t i = 0; i < subCount; i++) {
        auto subGraphDef = graphDef->mutable_sub_graphs(i);
        auto domain = getGraphDomain(subGraphDef->graph_id());
        auto subGraph = createSubGraph(subGraphDef, domain);
        if (!subGraph) {
            NAVI_LOG(ERROR, "create graph failed");
            return EC_UNKNOWN;
        }
        addSubGraph(subGraph);
        auto ec = subGraph->init();
        if (EC_NONE != ec) {
            return ec;
        }
    }
    return EC_NONE;
}

SubGraphBase *Graph::createSubGraph(SubGraphDef *subGraphDef,
                                    const GraphDomainPtr &domain)
{
    auto graphId = subGraphDef->graph_id();
    auto borderMap = getBorderMap(graphId);
    assert(borderMap);
    auto type = domain->getType();
    switch (type) {
    case GDT_LOCAL:
    case GDT_FORK:
    {
        auto it = _rewriteInfoMap.find(graphId);
        if (_rewriteInfoMap.end() == it) {
            NAVI_LOG(ERROR, "rewrite info not found for sub graph [%d]", graphId);
            return nullptr;
        }
        return new LocalSubGraph(subGraphDef, _param, domain, borderMap, this, it->second);
    }
    case GDT_CLIENT:
    case GDT_SERVER:
    case GDT_USER:
        return new RemoteSubGraphBase(subGraphDef, _param, domain, borderMap, this);
    default:
        return nullptr;
    }
}

bool Graph::validateInputResource(const BizPtr &biz) const {
    if (_param->worker->getTestMode() != TM_NOT_TEST) {
        return true;
    }
    bool ret = true;
    const auto &map = _inputResourceMap.getMap();
    for (const auto &pair : map) {
        const auto &name = pair.first;
        auto stage = biz->getResourceStage(name);
        if (RS_UNKNOWN != stage && RS_EXTERNAL != stage && RS_RUN_GRAPH_EXTERNAL != stage) {
            NAVI_LOG(ERROR, "invalid input stage [%s], resource [%s]", ResourceStage_Name(stage).c_str(), name.c_str());
            ret = false;
        }
    }
    return ret;
}

bool Graph::tryInitThisPartIdFromPartIds(SubGraphDef *subGraphDef,
                                         bool &isLocalGraph,
                                         NaviPartId &partCount,
                                         NaviPartId &partId) {
    auto graphId = subGraphDef->graph_id();
    auto domain = getGraphDomain(graphId);
    if (domain) {
        auto type = domain->getType();
        if (GDT_SERVER == type) {
            isLocalGraph = false;
            return true;
        } else if (GDT_FORK == type) {
            auto forkDomain = dynamic_cast<GraphDomainFork *>(domain.get());
            if (!forkDomain) {
                NAVI_LOG(ERROR, "get fork domain failed");
                return false;
            }
            isLocalGraph = true;
            partCount = forkDomain->getPartCount();
            partId = forkDomain->getPartId();
            initThisPartId(subGraphDef);
            return true;
        } else {
            NAVI_LOG(ERROR, "invalid domain type [%d]", type);
            return false;
        }
    }
    auto location = subGraphDef->mutable_location();
    auto thisPartId = location->this_part_id();
    auto partInfo = location->mutable_part_info();
    if (INVALID_NAVI_PART_ID == thisPartId) {
        if (1 == partInfo->part_ids_size()) {
            location->set_this_part_id(partInfo->part_ids(0));
        }
    }
    isLocalGraph = isLocal(*location);
    if (isLocalGraph) {
        _localBizName = location->biz_name();
        _localPartId = location->this_part_id();
    }
    initThisPartId(subGraphDef);
    partCount = location->part_info().part_count();
    partId = location->this_part_id();
    return true;
}

void Graph::initThisPartId(SubGraphDef *subGraphDef) {
    auto location = subGraphDef->mutable_location();
    if (INVALID_NAVI_PART_ID == location->this_part_id()) {
        location->set_this_part_id(0);
    }
}

void Graph::addSubGraph(SubGraphBase *subGraph) {
    _subGraphs.push_back(subGraph);
}

bool Graph::initBorder() {
    GraphBorderPtr border(new GraphBorder(_logger.logger, _param));
    for (const auto &pair : _subGraphDomainMap) {
        if (!border->collectSubGraph(*pair.first, pair.second)) {
            return false;
        }
    }
    if (!border->weave()) {
        return false;
    }
    _border = border;
    return true;
}

ErrorCode Graph::initNaviResult() {
    if (getGraphDomain(PARENT_GRAPH_ID)) {
        return EC_NONE;
    }
    const auto &result = _param->worker->getUserResult();
    auto domain = getGraphDomain(USER_GRAPH_ID);
    if (!domain) {
        return EC_NONE;
    }
    if (GDT_SERVER == domain->getType()) {
        return EC_NONE;
    }
    auto userDomain = std::dynamic_pointer_cast<GraphDomainUser>(domain);
    if (!userDomain) {
        NAVI_LOG(ERROR, "invalid domain type, expect GraphDomainUser [%p]", domain.get());
        return EC_INIT_GRAPH;
    }
    if (!result->bindDomain(userDomain)) {
        NAVI_LOG(ERROR, "result bind domain failed");
        return EC_INIT_GRAPH;
    }
    return EC_NONE;
}

ErrorCode Graph::run() {
    if (!validOverrideData()) {
        return EC_OVERRIDE_EDGE_DATA;
    }
    if (!initTimer()) {
        return EC_INIT_GRAPH;
    }
    autil::ScopedLock lock(_terminateLock);
    for (auto subGraph : _subGraphs) {
        auto ec = subGraph->run();
        if (EC_NONE != ec) {
            return ec;
        }
    }
    if (!runGraphDomains()) {
        return EC_INIT_GRAPH;
    }
    return EC_NONE;
}

bool Graph::validOverrideData() const {
    std::set<GraphId> graphIdSet;
    for (auto subGraph : _subGraphs) {
        graphIdSet.insert(subGraph->getGraphId());
    }
    const auto &overrideDatas = _param->runParams.getOverrideDatas();
    auto ret = true;
    for (const auto &overrideData : overrideDatas) {
        auto graphId = overrideData.graphId;
        if (graphIdSet.end() == graphIdSet.find(graphId)) {
            NAVI_LOG(ERROR,
                     "override graph id [%d] not exist, output node [%s], "
                     "output port [%s]",
                     graphId, overrideData.outputNode.c_str(),
                     overrideData.outputPort.c_str());
            ret = false;
        }
    }
    return ret;
}

void Graph::notifyTimeout() {
    if (_parent) {
        NAVI_LOG(ERROR, "fork graph timeout");
    } else {
        NAVI_LOG(ERROR, "session timeout");
    }
    notifyFinish(EC_TIMEOUT);
}

void Graph::timeoutCallback(void *data, bool isTimeout) {
    auto graph = (Graph *)data;
    auto worker = graph->getGraphParam()->worker;
    if (isTimeout) {
        auto item = new TimeoutItem(worker, graph);
        worker->schedule(item, true);
    } else {
        worker->decItemCount();
    }
}

bool Graph::initTimer() {
    autil::ScopedLock lock(_timerLock);
    auto worker = _param->worker;
    auto timeoutMs = _param->runParams.getTimeoutMs();
    if (timeoutMs >= DEFAULT_TIMEOUT_MS) {
        NAVI_KERNEL_LOG(SCHEDULE1, "timeoutMs[%ld] not set or too large, skip add timer", timeoutMs);
        return true;
    }
    _timeoutChecker.setTimeoutMs(timeoutMs);
    NAVI_KERNEL_LOG(SCHEDULE1, "set timeout checker with timeout [%ld]ms", timeoutMs);
    auto evTimer = worker->getEvTimer();
    if (!evTimer) {
        NAVI_KERNEL_LOG(ERROR, "get evTimer from worker failed");
        return false;
    }
    worker->incItemCount();
    NAVI_KERNEL_LOG(SCHEDULE1, "add timer with timeout [%ld]ms", timeoutMs);
    _timer = evTimer->addTimer(timeoutMs / 1000.0f, timeoutCallback, this);
    return true;
}

bool Graph::isTimeout() const { return _timeoutChecker.timeout(); }

int64_t Graph::getRemainTimeMs() const { return _timeoutChecker.remainTime(); }

bool Graph::updateTimeoutMs(int64_t timeoutMs) {
    autil::ScopedLock lock(_timerLock);
    auto remainTimeMs = getRemainTimeMs();
    if (timeoutMs > remainTimeMs) {
        NAVI_KERNEL_LOG(
            DEBUG, "not allow update a larger timeout value[%ld ms], remain time[%ld ms]", timeoutMs, remainTimeMs);
        return false;
    }
    _param->runParams.setTimeoutMs(timeoutMs);
    _timeoutChecker.setTimeoutMs(timeoutMs);
    auto worker = _param->worker;
    auto evTimer = worker->getEvTimer();
    if (!evTimer) {
        NAVI_KERNEL_LOG(ERROR, "get evTimer from worker failed");
        return false;
    }
    if (_timer) {
        evTimer->updateTimer(_timer, timeoutMs / 1000.0f);
    } else {
        worker->incItemCount();
        _timer = evTimer->addTimer(timeoutMs / 1000.0f, timeoutCallback, this);
    }
    return true;
}

const TimeoutChecker *Graph::getTimeoutChecker() const { return &_timeoutChecker; }

void Graph::stopTimer() {
    autil::ScopedLock lock(_timerLock);
    if (_timer) {
        auto evTimer = _param->worker->getEvTimer();
        evTimer->stopTimer(_timer);
        _timer = nullptr;
    }
}

void Graph::setGraphDomain(GraphId graphId, const GraphDomainPtr &domain) {
    _graphDomainMap->emplace(graphId, domain);
}

void Graph::notifyFinish(ErrorCode ec) {
    notifyFinish(ec, GFT_THIS_FINISH);
}

void Graph::notifyFinish(ErrorCode ec, GraphFinishType type) {
    NAVI_LOG(SCHEDULE1, "finish, parent [%p], ec [%s], type [%d]", _parent,
             CommonUtil::getErrorString(ec), type);
    bool terminateThis =
        (EC_NONE != ec || GFT_THIS_FINISH == type || GFT_PARENT_FINISH == type);
    if (terminateThis) {
        bool expect = false;
        if (!_terminated.compare_exchange_weak(expect, true)) {
            return;
        }
        auto worker = _param->worker;
        worker->incItemCount();
        stopTimer();
        {
            autil::ScopedLock lock(_terminateLock);
            if (!_parent) {
                worker->setFinish(ec);
            }
            terminateAll(ec);
            if (_parent) {
                auto terminateParent = (EC_NONE != ec);
                auto notifyParent =
                    terminateParent && GFT_PARENT_FINISH != type;
                if (notifyParent) {
                    if (!_errorAsEof) {
                        NAVI_LOG(DEBUG, "notify parent, errorAsEof [%d]", _errorAsEof);
                        _parent->notifyFinish(ec, GFT_THIS_FINISH);
                    }
                }
            }
            if (!_parent) {
                worker->fillResult();
            }
        }
        worker->decItemCount();
    }
}

void Graph::terminateAll(ErrorCode ec) {
    for (auto subGraph : _subGraphs) {
        subGraph->terminateThis(ec);
    }
    const auto &bizDomainMap = *_graphDomainMap;
    for (const auto &pair : bizDomainMap) {
        pair.second->notifyFinish(ec, false);
    }
}

SubGraphBorderMapPtr Graph::getBorderMap(GraphId graphId) const {
    return _border->getBorderMap(graphId);
}

GraphParam *Graph::getGraphParam() const {
    return _param;
}

void Graph::setResourceMap(const ResourceMap *resourceMap) {
    if (resourceMap) {
        _inputResourceMap.addResource(*resourceMap);
    }
}

const std::string &Graph::getLocalBizName() const {
    if (_parent) {
        return _parent->getLocalBizName();
    }
    return _localBizName;
}

NaviPartId Graph::getLocalPartId() const {
    if (_parent) {
        return _parent->getLocalPartId();
    }
    return _localPartId;
}

bool Graph::fillInputResource(GraphId graphId, const ResourceMapPtr &resourceMap) {
    auto inputSubResourceMap = getSubResourceMap(graphId);
    if (inputSubResourceMap) {
        if (!resourceMap->addResource(*inputSubResourceMap)) {
            NAVI_LOG(ERROR, "override sub graph resource failed");
            return false;
        }
    }
    const auto &resources = _inputResourceMap.getMap();
    for (const auto &pair : resources) {
        if (!resourceMap->addResource(pair.second)) {
            NAVI_LOG(ERROR, "resource override [%s]",
                     pair.second->getName().c_str());
            return false;
        }
    }
    resourceMap->addResource(std::make_shared<TimeoutCheckerR>(&_timeoutChecker));
    resourceMap->addResource(std::make_shared<QuerySessionR>(_param->querySession));
    resourceMap->addResource(std::make_shared<NaviResultR>(_param->worker->getUserResult()->getNaviResult()));

    NAVI_LOG(SCHEDULE1, "input resource map [%s]",
             autil::StringUtil::toString(*resourceMap).c_str());
    return true;
}

bool Graph::isLocal(const LocationDef &location) const {
    auto ret = _param->bizManager->isLocal(location);
    NAVI_LOG(SCHEDULE2, "location: %s, isLocal: %d",
             location.DebugString().c_str(), ret);
    return ret;
}

const std::string &Graph::getBizName(const SubGraphDef &subGraphDef) {
    return subGraphDef.location().biz_name();
}

GraphDomainPtr Graph::getGraphDomain(GraphId graphId) const {
    auto it = _graphDomainMap->find(graphId);
    if (_graphDomainMap->end() != it) {
        return it->second;
    } else {
        return nullptr;
    }
}

BizPtr Graph::getBiz(const SubGraphDef &subGraphDef) {
    auto graphId = subGraphDef.graph_id();
    if (PARENT_GRAPH_ID == graphId) {
        auto forkDomain = getForkDomain();
        if (forkDomain){
            return forkDomain->getBiz();
        }
    }
    return doGetBiz(subGraphDef);
}

GraphDomainFork *Graph::getForkDomain() const {
    auto domain = getGraphDomain(PARENT_GRAPH_ID);
    return dynamic_cast<GraphDomainFork *>(domain.get());
}

BizPtr Graph::doGetBiz(const SubGraphDef &subGraphDef) {
    auto bizManager = _param->bizManager;
    return bizManager->getBiz(_param->logger.get(), getBizName(subGraphDef));
}

void Graph::setErrorAsEof(bool errorAsEof) {
    _errorAsEof = errorAsEof;
}

void Graph::setSubResourceMap(const std::map<GraphId, ResourceMapPtr> &subResourceMaps) {
    _subResourceMaps = subResourceMaps;
}

ResourceMapPtr Graph::getSubResourceMap(GraphId graphId) const {
    auto it = _subResourceMaps.find(graphId);
    if (_subResourceMaps.end() != it) {
        return it->second;
    }
    return nullptr;
}

}
