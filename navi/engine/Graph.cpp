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
#include "navi/resource/GigQuerySessionR.h"
#include "navi/resource/MetaInfoResourceBase.h"
#include "navi/resource/TimeoutCheckerR.h"
#include "navi/util/CommonUtil.h"

using namespace multi_call;

namespace navi {

Graph::Graph(GraphParam *param, Graph *parent)
    : _logger(this, "graph", param->logger)
    , _pool(param->pool)
    , _param(param)
    , _graphDomainMap(new GraphDomainMap())
    , _parent(parent)
    , _terminated(false)
{
}

Graph::~Graph() {
    for (auto subGraph : _subGraphs) {
        NAVI_POOL_DELETE_CLASS(subGraph);
    }
}

ErrorCode Graph::init(GraphDef *graphDef) {
    NAVI_LOG(SCHEDULE1, "graph def [%s]", graphDef->DebugString().c_str());
    autil::ScopedLock lock(_terminateLock);
    if (!preInitGraphDomains(graphDef)) {
        return EC_INIT_GRAPH;
    }
    if (!initBorder()) {
        return EC_INIT_GRAPH;
    }
    if (!postInitGraphDomains()) {
        return EC_INIT_GRAPH;
    }
    auto ec = createSubGraphs(graphDef);
    if (EC_NONE != ec) {
        return ec;
    }

    NAVI_LOG(SCHEDULE1,
             "after init graph def [%s], graph border [%s]",
             graphDef->DebugString().c_str(),
             autil::StringUtil::toString(*_border).c_str());

    return initNaviResult();
}

bool Graph::preInitGraphDomains(GraphDef *graphDef) {
    auto &bizDomainMap = *_graphDomainMap;
    auto subCount = graphDef->sub_graphs_size();
    for (int32_t i = 0; i < subCount; i++) {
        auto subGraphDef = graphDef->mutable_sub_graphs(i);
        const auto &location = subGraphDef->location();
        const auto &bizName = location.biz_name();
        if (isLocal(location)) {
            _localBizName = bizName;
            _localPartId = location.this_part_id();
        }
        auto graphId = subGraphDef->graph_id();
        GraphDomainPtr domain;
        auto it = bizDomainMap.find(graphId);
        if (bizDomainMap.end() != it) {
            domain = it->second;
        } else {
            domain = createGraphDomain(graphId, location);
            bizDomainMap.emplace(graphId, domain);
        }
        bindGraphDomain(domain, subGraphDef);
        initThisPartId(subGraphDef);
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
    } else if (isLocal(location)) {
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
    domain->addSubGraph(subGraphDef);
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
    auto borderMap = getBorderMap(subGraphDef->graph_id());
    assert(borderMap);
    auto type = domain->getType();
    switch (type) {
    case GDT_LOCAL:
    case GDT_FORK:
    {
        auto biz = getBiz(domain, *subGraphDef);
        auto resourceMap = getResourceMap(*subGraphDef);
        if (!biz || !resourceMap) {
            NAVI_LOG(ERROR,
                     "get biz or resource map failed, biz[%p] resourceMap [%p] "
                     "subGraph [%s]",
                     biz.get(), resourceMap.get(),
                     subGraphDef->DebugString().c_str());
            return nullptr;
        }
        if (!fillInputResource(resourceMap)) {
            return nullptr;
        }
        return NAVI_POOL_NEW_CLASS(_pool, LocalSubGraph, subGraphDef, _param,
                                   domain, borderMap, this, biz, resourceMap);
    }
    case GDT_CLIENT:
    case GDT_SERVER:
    case GDT_USER:
        return NAVI_POOL_NEW_CLASS(_pool, RemoteSubGraphBase, subGraphDef,
                                   _param, domain, borderMap, this);
    default:
        return nullptr;
    }
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
    auto result = _param->worker->getUserResult();
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

void Graph::setOverrideData(const std::vector<OverrideData> &overrideData) {
    _overrideData = overrideData;
    for (auto subGraph : _subGraphs) {
        subGraph->setOverrideData(overrideData);
    }
}

const std::vector<OverrideData> &Graph::getOverrideData() const {
    return _overrideData;
}

ErrorCode Graph::run() {
    bool forkGraph = isForkGraph();
    if (!forkGraph && !validOverrideData()) {
        return EC_OVERRIDE_EDGE_DATA;
    }
    autil::ScopedLock lock(_terminateLock);
    for (auto subGraph : _subGraphs) {
        auto ec = subGraph->run(forkGraph);
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
    const auto &overrideDatas =
        _param->worker->getRunParams().getOverrideDatas();
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
                    _parent->notifyFinish(ec, GFT_THIS_FINISH);
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

bool Graph::fillInputResource(const ResourceMapPtr &resourceMap) {
    resourceMap->addResource(_param->worker->getGraphMemoryPoolResource());
    resourceMap->addResource(
        ResourcePtr(new TimeoutCheckerR(_param->worker->getTimeoutChecker())));
    resourceMap->addResource(ResourcePtr(new GigQuerySessionR(_param->querySession)));
    const auto &metaInfoResource = _param->worker->getMetaInfoResource();
    if (metaInfoResource) {
        resourceMap->addResource(metaInfoResource);
    }
    auto gigClientResource = _param->bizManager->getRootResource(GIG_CLIENT_RESOURCE_ID);
    if (gigClientResource) {
        resourceMap->addResource(std::move(gigClientResource));
    }

    const auto &resources = _inputResourceMap.getMap();
    for (const auto &pair : resources) {
        if (!resourceMap->addResource(pair.second)) {
            NAVI_LOG(ERROR, "resource override [%s]",
                     pair.second->getName().c_str());
            return false;
        }
    }
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

BizPtr Graph::getBiz(const GraphDomainPtr &domain,
                     const SubGraphDef &subGraphDef)
{
    auto forkDomain = dynamic_cast<GraphDomainFork *>(domain.get());
    if (!forkDomain) {
        return getBiz(subGraphDef);
    } else {
        return forkDomain->getBiz();
    }
}

BizPtr Graph::getBiz(const SubGraphDef &subGraphDef) {
    auto bizManager = _param->bizManager;
    return bizManager->getBiz(_param->logger.get(), getBizName(subGraphDef));
}

ResourceMapPtr Graph::getResourceMap(const SubGraphDef &subGraphDef) {
    return _param->resourceManager.getBizResourceMap(subGraphDef.location());
}

}
