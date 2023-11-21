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
        auto ec = EC_INIT_GRAPH;
        setErrorCode(ec);
        return ec;
    }
    return EC_NONE;
}

bool LocalSubGraph::doInit() {
    NaviLoggerScope scope(_logger);
    initSubGraphOption();
    if (!initScopeMap()) {
        return false;
    }
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
    bool collectPerf = _param->runParams.collectPerf();
    if (inlineCompute(worker, node)) {
        worker->inlineBegin();
        NAVI_LOG(DEBUG, "inline compute, node [%s] kernel [%s]",
                 node->getName().c_str(), node->getKernelName().c_str());
        auto schedInfo = worker->makeInlineSchedInfo();
        bool enableSuccess = false;
        if (collectPerf) {
            enableSuccess = worker->enablePerf();
        }
        node->compute(schedInfo);
        if (collectPerf && enableSuccess) {
            worker->disablePerf();
        }
        worker->inlineEnd();
        return true;
    }
    auto item = new KernelWorkItem(worker, node, collectPerf);
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
            auto error =
                _param->graphResult->makeError(_logger.logger, EC_UNKNOWN);
            notifyFinish(node, error);
        }
    }
    return true;
}

bool LocalSubGraph::inlineCompute(NaviWorkerBase * worker,
                                  Node * node)
{
    if (unlikely(getTestMode() != TM_NONE)) {
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
    _errorHandleStrategy = _subGraphDef->option().error_handle_strategy();
}

bool LocalSubGraph::initScopeMap() {
    auto count = _subGraphDef->scopes_size();
    for (int32_t i = 0; i < count; ++i) {
        const auto &scopeDef = _subGraphDef->scopes(i);
        auto id = scopeDef.id();
        auto it = _scopeInfoMap.find(id);
        if (_scopeInfoMap.end() != it) {
            NAVI_LOG(ERROR, "duplicate scope id [%d]", id);
            return false;
        }
        auto scopeInfo = &_scopeInfoMap[id];
        scopeInfo->def = &scopeDef;
        if (!scopeDef.has_terminator()) {
            scopeInfo->logger = _logger.logger;
        } else {
            scopeInfo->logger = std::make_shared<NaviLogger>(*_logger.logger);
        }
    }
    return true;
}

ScopeInfo *LocalSubGraph::getScopeInfo(int32_t scopeIndex) {
    auto it = _scopeInfoMap.find(scopeIndex);
    if (_scopeInfoMap.end() == it) {
        NAVI_LOG(ERROR, "scope not exist id [%d]", scopeIndex);
        return nullptr;
    }
    return &it->second;
}

void LocalSubGraph::addToScopeInfo(ScopeInfo *scopeInfo, NodeType nodeType,
                                   Node *node)
{
    if (NT_SCOPE_TERMINATOR == nodeType) {
        scopeInfo->terminator = node;
    } else if (NT_SCOPE_TERMINATOR_SPLIT != nodeType && NT_TERMINATOR != nodeType) {
        scopeInfo->nodes.push_back(node);
    }
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

void LocalSubGraph::setScopeError(int32_t scopeIndex,
                                  const NaviErrorPtr &error)
{
    auto scopeInfoPtr = getScopeInfo(scopeIndex);
    if (!scopeInfoPtr) {
        return;
    }
    auto &scopeInfo = *scopeInfoPtr;
    scopeInfo.setError(error);
    if (!scopeInfo.terminator) {
        _param->graphResult->updateError(error);
        return;
    }
}

void LocalSubGraph::notifyFinish(Node *node, const NaviErrorPtr &error) {
    auto def = node->getDef();
    auto nodeType = def->type();
    auto scope = def->scope();
    if (NT_SCOPE_TERMINATOR == nodeType || NT_SCOPE_TERMINATOR_SPLIT == nodeType) {
        terminateScope(node, scope, error);
    } else if (NT_TERMINATOR != nodeType) {
        if (terminateScope(node, scope, error)) {
            return;
        }
    }
    if ((error && EC_NONE != error->ec) || isForkDomain()) {
        if (EHS_ERROR_AS_FATAL == _errorHandleStrategy) {
            _graph->notifyFinish(error);
        } else {
            forceStopNodes(node, _nodeVec);
        }
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
    auto scopeInfo = getScopeInfo(nodeDef.scope());
    if (!scopeInfo) {
        NAVI_LOG(ERROR, "get scope info failed, node [%s]", nodeName.c_str());
        return nullptr;
    }
    auto isResourceNode = (nodeDef.kernel_name() == RESOURCE_CREATE_KERNEL);
    Node *node =
        new Node(nodeDef, scopeInfo->logger, _biz.get(), this, isResourceNode);
    if (!node->init()) {
        delete node;
        return nullptr;
    }
    _nodeMap.insert(std::make_pair(nodeName, node));
    _nodeVec.emplace_back(node);
    addToScopeInfo(scopeInfo, nodeDef.type(), node);
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

void LocalSubGraph::collectMetric(GraphMetric *graphMetric) {
    const auto &bizName = getBizName();
    for (const auto &node : _nodeVec) {
        const auto &metric = node->getMetric()->cloneAndClearEvent();
        graphMetric->addKernelMetric(bizName, metric);
    }
}

void LocalSubGraph::collectForkGraphMetric() {
    for (const auto &node : _nodeVec) {
        node->collectForkGraphMetric();
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

bool LocalSubGraph::terminateScope(Node *node, int32_t scopeIndex,
                                   const NaviErrorPtr &error)
{
    auto scopeInfoPtr = getScopeInfo(scopeIndex);
    if (!scopeInfoPtr) {
        return false;
    }
    auto &scopeInfo = *scopeInfoPtr;
    if (!scopeInfo.terminator) {
        return false;
    }
    bool expect = false;
    if (!scopeInfo.terminated.compare_exchange_weak(expect, true)) {
        return true;
    }
    scopeInfo.setError(error);
    NAVI_MEMORY_BARRIER();
    forceStopNodes(node, scopeInfo.nodes);
    return true;
}

void LocalSubGraph::forceStopNodes(Node *stopBy, const std::vector<Node *> &nodes) {
    for (auto n : nodes) {
        n->setForceStopFlag(stopBy);
    }
    for (auto n : nodes) {
        n->forceStop();
    }
}

NaviErrorPtr LocalSubGraph::getScopeError(int32_t scopeIndex) {
    auto scopeInfoPtr = getScopeInfo(scopeIndex);
    if (!scopeInfoPtr) {
        return nullptr;
    }
    return scopeInfoPtr->getError();
}

ErrorHandleStrategy LocalSubGraph::getScopeErrorHandleStrategy(
        int32_t scopeIndex)
{
    auto scopeInfoPtr = getScopeInfo(scopeIndex);
    if (!scopeInfoPtr) {
        return EHS_ERROR_AS_FATAL;
    }
    return scopeInfoPtr->def->error_handle_strategy();
}

TestMode LocalSubGraph::getTestMode() const { return _param->worker->getTestMode(); }

}
