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
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/Edge.h"
#include "navi/engine/Graph.h"
#include "navi/engine/GraphDomainFork.h"
#include "navi/engine/KernelWorkItem.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/Node.h"
#include "navi/util/CommonUtil.h"
#include <unistd.h>

namespace navi {

thread_local extern size_t current_thread_counter;
thread_local extern size_t current_thread_wait_counter;

LocalSubGraph::LocalSubGraph(SubGraphDef *subGraphDef,
                             GraphParam *param,
                             const GraphDomainPtr &domain,
                             const SubGraphBorderMapPtr &borderMap,
                             Graph *graph,
                             const RewriteInfoPtr &rewriteInfo)
    : SubGraphBase(subGraphDef, param, domain, borderMap, graph)
    , _biz(rewriteInfo->biz)
    , _rewriteInfo(rewriteInfo)
    , _finishPortIndex(0)
    , _frozen(true)
    , _inlineMode(false)
{
    _logger.addPrefix("local");
}

LocalSubGraph::~LocalSubGraph() {
    for (const auto &node : _nodeVec) {
        delete node;
    }
    for (const auto &pair : _edges) {
        delete pair.second;
    }
}

ErrorCode LocalSubGraph::init() {
    if (!doInit()) {
        return EC_INIT_GRAPH;
    }
    return EC_NONE;
}

bool LocalSubGraph::doInit() {
    NaviLoggerScope scope(_logger);
    initSubGraphOption();
    if (!createNodes()) {
        return false;
    }
    if (!bindPortToNode()) {
        return false;
    }
    if (!createEdges()) {
        return false;
    }
    if (!bindForkDomain()) {
        return false;
    }
    if (!initPortDataType()) {
        return false;
    }
    if (!postInit()) {
        return false;
    }
    finishPendingOutput();
    if (!checkGraph()) {
        return false;
    }
    return true;
}

ErrorCode LocalSubGraph::run() {
    _frozen = false;
    auto ec = flushEdgeOverride();
    if (EC_NONE != ec) {
        return ec;
    }
    for (const auto &node : _nodeVec) {
        bool start = false;
        if (0 == node->outputDegree()) {
            start = true;
        }
        if (node->isBorder()) {
            start = true;
        }
        if (start) {
            node->startSchedule(node);
        }
    }
    return EC_NONE;
}

ErrorCode LocalSubGraph::flushEdgeOverride() {
    const auto &overrideDatas = _param->runParams.getOverrideDatas();
    auto subGraphId = getGraphId();
    auto partId = getPartId();
    for (const auto &overrideData : overrideDatas) {
        if (overrideData.graphId != subGraphId) {
            continue;
        }
        auto overridePartId = overrideData.partId;
        if (INVALID_NAVI_PART_ID != overridePartId && partId != overridePartId) {
            continue;
        }
        auto edgeKey = CommonUtil::getPortKey(overrideData.outputNode,
                                              overrideData.outputPort);
        auto it = _edges.find(edgeKey);
        if (_edges.end() == it) {
            NAVI_LOG(ERROR,
                     "override edge data failed, graphId [%d] partId [%d] node [%s] port[%s] edgeKey[%s] not exist",
                     overrideData.graphId,
                     overrideData.partId,
                     overrideData.outputNode.c_str(),
                     overrideData.outputPort.c_str(),
                     edgeKey.c_str());

            return EC_OVERRIDE_EDGE_DATA;
        }
        if (!it->second->overrideData(overrideData.datas)) {
            NAVI_LOG(ERROR,
                     "multiple edge override data, graphId [%d] partId [%d] node [%s] port[%s]",
                     overrideData.graphId,
                     overrideData.partId,
                     overrideData.outputNode.c_str(),
                     overrideData.outputPort.c_str());
            return EC_OVERRIDE_EDGE_DATA;
        }
    }
    return EC_NONE;
}

bool LocalSubGraph::schedule(Node *node) {
    if (_frozen) {
        NAVI_LOG(SCHEDULE1,
                 "graph is frozen, node [%s] kernel [%s] schedule ignored",
                 node->getName().c_str(), node->getKernelName().c_str());
        return false;
    }
    if (terminated()) {
        NAVI_LOG(SCHEDULE1,
                 "graph terminated, node [%s] kernel [%s] schedule ignored",
                 node->getName().c_str(), node->getKernelName().c_str());
        return true;
    }
    auto worker = _param->worker;
    if (inlineCompute(worker, node)) {
        worker->inlineBegin();
        NAVI_LOG(DEBUG, "inline compute, node [%s] kernel [%s]",
                 node->getName().c_str(), node->getKernelName().c_str());
        auto time = CommonUtil::getTimelineTimeNs();
        auto threadId = current_thread_id;
        ScheduleInfo schedInfo;
        schedInfo.enqueueTime = time;
        schedInfo.dequeueTime = time;
        schedInfo.schedTid = threadId;
        schedInfo.signalTid = threadId;
        schedInfo.processTid = threadId;
        schedInfo.runningThread = worker->getRunningThreadCount();
        schedInfo.threadCounter = current_thread_counter;
        schedInfo.threadWaitCounter = current_thread_wait_counter;
        schedInfo.queueSize = worker->getQueueSize();
        node->compute(schedInfo);
        worker->inlineEnd();
        return true;
    }
    auto item = new KernelWorkItem(worker, node);
    auto ret = worker->schedule(item);
    if (unlikely(!ret)) {
        if (worker->isFinished()) {
            NAVI_LOG(DEBUG, "ignore schedule failure after worker finshed, node[%s]",
                     node->getName().c_str());
        } else if (_graph->isTimeout()) {
            _graph->notifyTimeout();
        } else {
            NAVI_LOG(ERROR, "schedule kernel failed, node[%s]",
                     node->getName().c_str());
            notifyFinish(node, EC_UNKNOWN);
        }
    }
    return true;
}

bool LocalSubGraph::inlineCompute(NaviWorkerBase * worker,
                                  Node * node)
{
    if (unlikely(getTestMode() != TM_NOT_TEST)) {
        return true;
    }
    if (!worker->canInline()) {
        return false;
    }
    if (_inlineMode) {
        return true;
    }
    auto canInline = node->isInline();
    if (!canInline) {
        return false;
    }
    if (worker->getIdleThreadCount() > 0) {
        return false;
    }
    return true;
}

void LocalSubGraph::initSubGraphOption() {
    _inlineMode = _subGraphDef->option().inline_mode();
}

bool LocalSubGraph::createNodes() {
    const auto &subGraph = *_subGraphDef;
    auto nodeCount = subGraph.nodes_size();
    for (auto i = 0; i < nodeCount; i++) {
        const auto &node = subGraph.nodes(i);
        if (!createNode(node)) {
            NAVI_LOG(ERROR, "create node[%s] fail", node.name().c_str());
            return false;
        }
    }
    return true;
}

bool LocalSubGraph::createEdges() {
    const auto &subGraph = *_subGraphDef;
    auto edgeCount = subGraph.edges_size();
    for (auto i = 0; i < edgeCount; i++) {
        if (!createEdge(subGraph.edges(i))) {
            const auto &edgeDef = subGraph.edges(i);
            NAVI_LOG(ERROR, "create edge[%s:%s -> %s:%s] fail",
                     edgeDef.input().node_name().c_str(),
                     edgeDef.input().port_name().c_str(),
                     edgeDef.output().node_name().c_str(),
                     edgeDef.output().port_name().c_str());
            return false;
        }
    }
    return true;
}

bool LocalSubGraph::bindForkDomain() {
    auto domain = dynamic_cast<GraphDomainFork *>(_domain.get());
    if (!domain) {
        return true;
    }
    return domain->bindGraphEdgeList(_edges);
}

bool LocalSubGraph::isForkDomain() const {
    return nullptr != dynamic_cast<GraphDomainFork *>(_domain.get());
}

bool LocalSubGraph::initPortDataType() {
    const auto &borderMap = *_borderMap;
    for (const auto &pair : borderMap) {
        const auto &borderId = pair.first;
        const auto &border = *pair.second;
        const auto &portVec = border.getPortVec();
        for (auto port : portVec) {
            if (!setPortType(port, borderId.ioType)) {
                NAVI_LOG(ERROR, "init port data type failed, portKey: %s",
                         port->getPortKey().c_str());
                return false;
            }
        }
    }
    return true;
}

bool LocalSubGraph::setPortType(Port *port, IoType ioType) {
    auto dataType = port->getDataType();
    if (dataType) {
        port->bindDataType(dataType);
        return true;
    }
    const auto &nodeName = port->getNode();
    auto node = getNode(nodeName);
    if (!node) {
        if (NAVI_FORK_NODE == nodeName) {
            return true;
        }
        return false;
    }
    return bindPortDataType(node, port, ioType);
}

bool LocalSubGraph::bindPortDataType(Node *node, Port *port, IoType ioType) {
    auto typeStr = getNodePortDataTypeStr(node, port->getPort(),
                                          port->getPeerPort(), ioType);
    if (typeStr.empty()) {
        return true;
    }
    auto type = _param->creatorManager->getType(typeStr);
    if (!type) {
        NAVI_LOG(ERROR,
                 "port data type [%s] not registered, node [%s] kernel [%s], "
                 "%s port [%s]",
                 typeStr.c_str(), node->getName().c_str(),
                 node->getKernelName().c_str(), CommonUtil::getIoType(ioType),
                 port->getPort().c_str());
    }
    port->bindDataType(type);
    return true;
}

std::string LocalSubGraph::getNodePortDataTypeStr(
        Node *node,
        const std::string &portName,
        const std::string &peerPortName,
        IoType ioType) const
{
    std::string typeStr;
    if ((0 == portName.find(NODE_CONTROL_INPUT_PORT)) || (0 == peerPortName.find(NODE_CONTROL_INPUT_PORT))) {
        typeStr = CONTROL_DATA_TYPE;
    } else {
        if (!node->getPortTypeStr(portName, ioType, typeStr)) {
            return "";
        }
    }
    return typeStr;
}

void LocalSubGraph::finishPendingOutput() {
    for (const auto &node : _nodeVec) {
        if (node->getName() != TERMINATOR_NODE_NAME) {
            node->finishPendingOutput();
        }
    }
}

bool LocalSubGraph::checkGraph() const {
    auto ret = checkNode();
    return checkEdge() && ret;
}

bool LocalSubGraph::checkNode() const {
    bool ret = true;
    for (const auto &node : _nodeVec) {
        if (node->getName() != TERMINATOR_NODE_NAME) {
            ret = node->checkConnection() && ret;
        }
    }
    return ret;
}

bool LocalSubGraph::checkEdge() const {
    bool ret = true;
    auto creatorManager = _param->creatorManager;
    for (const auto &pair : _edges) {
        ret = pair.second->checkType(creatorManager) && ret;
    }
    return ret;
}

bool LocalSubGraph::postInit() {
    for (const auto &node : _nodeVec) {
        if (!node->postInit()) {
            return false;
        }
    }
    for (const auto &pair : _edges) {
        pair.second->postInit();
    }
    return true;
}

Node *LocalSubGraph::getNode(const std::string &nodeName) const {
    auto it = _nodeMap.find(nodeName);
    if (it == _nodeMap.end()) {
        return nullptr;
    }
    return it->second;
}

void LocalSubGraph::notifyFinish(Node *node, ErrorCode ec) {
    auto def = node->getDef();
    auto nodeType = def->type();
    auto scope = def->scope();
    if (NT_SCOPE_TERMINATOR == nodeType || NT_SCOPE_TERMINATOR_SPLIT == nodeType) {
        terminateScope(node, scope, ec);
    } else if (NT_TERMINATOR != nodeType) {
        if (terminateScope(node, scope, ec)) {
            return;
        }
    }
    if (EC_NONE != ec || isForkDomain()) {
        _graph->notifyFinish(ec);
    }
}

NaviWorkerBase *LocalSubGraph::getWorker() const {
    return _param->worker;
}

const BizPtr &LocalSubGraph::getBiz() const {
    return _biz;
}

bool LocalSubGraph::saveResource(const ResourceMap &resourceMap) {
    ResourceMap fullMap(resourceMap.getMap());
    const auto &rootMap = _rewriteInfo->resourceMap->getMap();
    for (const auto &pair : rootMap) {
        fullMap.addResource(pair.second);
    }
    return _biz->saveResource(getPartCount(), getPartId(), fullMap);
}

bool LocalSubGraph::publishResource() {
    return _biz->publishResource(getPartCount(), getPartId());
}

void LocalSubGraph::collectResourceMap(
    MultiLayerResourceMap &multiLayerResourceMap) const
{
    multiLayerResourceMap.append(_rewriteInfo->multiLayerResourceMap);
}

ResourceStage LocalSubGraph::getResourceStage(const std::string &name) const {
    return _biz->getResourceStage(name);
}

const std::map<std::string, bool> *LocalSubGraph::getResourceDependResourceMap(const std::string &name) const {
    return _biz->getResourceDependResourceMap(name);
}

const std::unordered_set<std::string> *LocalSubGraph::getResourceReplacerSet(const std::string &name) const {
    return _biz->getResourceReplacerSet(name);
}

ErrorCode LocalSubGraph::createResource(const std::string &name,
                                        const MultiLayerResourceMap &multiLayerResourceMap,
                                        const ProduceInfo &produceInfo,
                                        NaviConfigContext *ctx,
                                        bool checkReuse,
                                        Node *requireKernelNode,
                                        ResourcePtr &resource) const {
    auto graphId = getGraphId();
    auto resourceStage = _param->runParams.getResourceStage();
    auto namedDataMap = _param->runParams.getSubNamedDataMap(graphId);
    return _biz->createResource(getPartCount(),
                                getPartId(),
                                name,
                                resourceStage,
                                multiLayerResourceMap,
                                namedDataMap,
                                produceInfo,
                                ctx,
                                checkReuse,
                                requireKernelNode,
                                resource);
}

ResourcePtr LocalSubGraph::buildKernelResourceRecur(const std::string &name,
                                                    KernelConfigContext *ctx,
                                                    const ResourceMap &nodeResourceMap,
                                                    Node *requireKernelNode,
                                                    ResourceMap &inputResourceMap)
{
    auto graphId = getGraphId();
    auto namedDataMap = _param->runParams.getSubNamedDataMap(graphId);
    return _biz->buildKernelResourceRecur(
        getPartCount(), getPartId(), name, ctx, namedDataMap, nodeResourceMap, requireKernelNode, inputResourceMap);
}

Node *LocalSubGraph::createNode(const NodeDef &nodeDef) {
    const auto &nodeName = nodeDef.name();
    if (_nodeMap.end() != _nodeMap.find(nodeName)) {
        NAVI_LOG(ERROR, "duplicate node [%s]", nodeName.c_str());
        return nullptr;
    }
    auto graphMetric = _param->graphMetric;
    auto metric = graphMetric->getKernelMetric(getBizName(), getGraphId(),
                                               nodeName, nodeDef.kernel_name());
    Node *node = new Node(_logger.logger, _biz.get(), metric, this);
    if (!node->init(nodeDef)) {
        delete node;
        return nullptr;
    }
    _nodeMap.insert(std::make_pair(nodeName, node));
    _nodeVec.emplace_back(node);
    addToScopeInfoMap(node);
    return node;
}

bool LocalSubGraph::createEdge(const EdgeDef &edgeDef) {
    const auto &inputDef = edgeDef.input();
    auto edgeKey = CommonUtil::getPortKey(inputDef.node_name(), inputDef.port_name());
    auto it = _edges.find(edgeKey);
    Edge *edge = nullptr;
    if (_edges.end() == it) {
        edge = new Edge(_logger.logger);
        if (!edge->init(*this, inputDef)) {
            delete edge;
            return false;
        }
        _edges.emplace(std::move(edgeKey), edge);
    } else {
        edge = it->second;
    }
    if (!edge->addOutput(*this, edgeDef.output(), edgeDef.require())) {
        return false;
    } else {
        return true;
    }
}

void LocalSubGraph::addToScopeInfoMap(Node *node) {
    auto def = node->getDef();
    auto nodeType = def->type();
    auto scope = def->scope();
    auto it = _scopeInfoMap.find(scope);
    ScopeInfo *scopeInfo = nullptr;
    if (_scopeInfoMap.end() == it) {
        scopeInfo = &_scopeInfoMap[scope];
    } else {
        scopeInfo = &it->second;
    }
    if (NT_SCOPE_TERMINATOR == nodeType) {
        scopeInfo->terminator = node;
    } else if (NT_SCOPE_TERMINATOR_SPLIT != nodeType && NT_TERMINATOR != nodeType) {
        scopeInfo->nodes.push_back(node);
    }
}

void LocalSubGraph::terminate(ErrorCode ec) {
    for (const auto &node : _nodeVec) {
        node->terminate(ec);
    }
}

bool LocalSubGraph::bindPortToNode() {
    const auto &borderMap = *_borderMap;
    for (const auto &pair : borderMap) {
        const auto &portVec = pair.second->getPortVec();
        for (auto port : portVec) {
            const auto &portNodeName = port->getPortNodeName();
            auto node = getNode(portNodeName);
            if (!node) {
                NAVI_LOG(ERROR, "port node [%s] not exist",
                         portNodeName.c_str());
                return false;
            }
            if (!node->bindPort(port)) {
                return false;
            }
        }
    }
    return true;
}

bool LocalSubGraph::terminateScope(Node *node, int32_t scopeIndex, ErrorCode ec) {
    auto it = _scopeInfoMap.find(scopeIndex);
    if (_scopeInfoMap.end() == it) {
        return false;
    }
    auto &scopeInfo = it->second;
    if (!scopeInfo.terminator) {
        return false;
    }
    bool expect = false;
    if (!scopeInfo.terminated.compare_exchange_weak(expect, true)) {
        return true;
    }
    const auto &nodes = scopeInfo.nodes;
    for (auto n : nodes) {
        n->forceStop(node);
    }
    scopeInfo.ec = ec;
    return true;
}

ErrorCode LocalSubGraph::getScopeErrorCode(int32_t scopeIndex) const {
    auto it = _scopeInfoMap.find(scopeIndex);
    if (_scopeInfoMap.end() == it) {
        return EC_NONE;
    }
    return it->second.ec;
}

TestMode LocalSubGraph::getTestMode() const { return _param->worker->getTestMode(); }

}
