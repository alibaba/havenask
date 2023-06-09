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
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/Edge.h"
#include "navi/engine/Graph.h"
#include "navi/engine/GraphDomainFork.h"
#include "navi/engine/KernelWorkItem.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/Node.h"
#include "navi/ops/ResourceKernelDef.h"
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
                             const BizPtr &biz,
                             const ResourceMapPtr &resourceMap)
    : SubGraphBase(subGraphDef, param, domain, borderMap, graph)
    , _biz(biz)
    , _resourceMap(resourceMap)
    , _finishPortIndex(0)
    , _frozen(true)
    , _inlineMode(false)
    , _testerMode(false)
{
    _logger.addPrefix("local");
}

LocalSubGraph::~LocalSubGraph() {
    for (const auto &node : _nodeVec) {
        NAVI_POOL_DELETE_CLASS(node);
    }
    for (const auto &pair : _edges) {
        NAVI_POOL_DELETE_CLASS(pair.second);
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
    NAVI_LOG(SCHEDULE2, "navi graph before: %s",
             _subGraphDef->DebugString().c_str());
    if (!addResourceNode()) {
        return false;
    }
    NAVI_LOG(SCHEDULE2, "navi graph after: %s",
             _subGraphDef->DebugString().c_str());
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

ErrorCode LocalSubGraph::run(bool isForkGraph) {
    _frozen = false;
    if (!isForkGraph || !_overrideData.empty()) {
        auto ec = flushEdgeOverride();
        if (EC_NONE != ec) {
            return ec;
        }
    }
    std::sort(_nodeVec.begin(), _nodeVec.end(), [](Node *left, Node *right) {
        return left->outputDegree() > right->outputDegree();
    });
    for (const auto &node : _nodeVec) {
        node->schedule(node);
    }
    return EC_NONE;
}

ErrorCode LocalSubGraph::flushEdgeOverride() {
    const auto &overrideDatas = _overrideData.empty()
                                ? _param->worker->getRunParams().getOverrideDatas()
                                : _overrideData;
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
                     "override edge data failed, node [%s] port[%s] "
                     "edgeKey[%s] not exist",
                     overrideData.outputNode.c_str(),
                     overrideData.outputPort.c_str(), edgeKey.c_str());

            return EC_OVERRIDE_EDGE_DATA;
        }
        if (!it->second->overrideData(overrideData.datas)) {
            NAVI_LOG(ERROR, "multiple edge override data, node [%s] port[%s]",
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
    auto item = NAVI_POOL_NEW_CLASS(_pool, KernelWorkItem, worker, node);
    auto ret = worker->schedule(item);
    if (unlikely(!ret)) {
        if (worker->isFinished()) {
            NAVI_LOG(DEBUG, "ignore schedule failure after worker finshed, node[%s]",
                     node->getName().c_str());
        } else if (worker->getTimeoutChecker()->timeout()) {
            notifyFinish(EC_TIMEOUT);
        } else {
            NAVI_LOG(ERROR, "schedule kernel failed, node[%s]",
                     node->getName().c_str());
            notifyFinish(EC_UNKNOWN);
        }
    }
    return true;
}

bool LocalSubGraph::inlineCompute(NaviWorkerBase * worker,
                                  Node * node)
{
    if (unlikely(_testerMode)) {
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
    _testerMode = _subGraphDef->option().tester_mode();
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
    if (NODE_CONTROL_INPUT_PORT == portName ||
        NODE_CONTROL_INPUT_PORT == peerPortName)
    {
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
        pair.second->postInit(_pool);
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

PoolPtr LocalSubGraph::getPool() const {
    return _param->pool;
}

void LocalSubGraph::notifyFinish(ErrorCode ec) {
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

bool LocalSubGraph::saveInitResource(const ResourceMap &resourceMap) {
    return _biz->saveInitResource(getPartCount(), getPartId(), resourceMap);
}

void LocalSubGraph::collectResourceMap(
    MultiLayerResourceMap &multiLayerResourceMap) const
{
    multiLayerResourceMap.addResourceMap(_resourceMap.get());
    return _biz->collectResourceMap(getPartCount(), getPartId(),
                                    multiLayerResourceMap);
}

ResourcePtr LocalSubGraph::createResource(
    const std::string &name,
    const MultiLayerResourceMap &multiLayerResourceMap) const
{
    return _biz->createResource(getPartCount(), getPartId(), name,
                                multiLayerResourceMap);
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
    Node *node = NAVI_POOL_NEW_CLASS(_pool, Node, _logger.logger, _biz.get(),
                                     metric, this);
    if (!node->init(nodeDef)) {
        NAVI_POOL_DELETE_CLASS(node);
        return nullptr;
    }
    _nodeMap.insert(std::make_pair(nodeName, node));
    _nodeVec.emplace_back(node);
    return node;
}

bool LocalSubGraph::createEdge(const EdgeDef &edgeDef) {
    const auto &inputDef = edgeDef.input();
    auto edgeKey = CommonUtil::getPortKey(inputDef.node_name(), inputDef.port_name());
    auto it = _edges.find(edgeKey);
    Edge *edge = nullptr;
    if (_edges.end() == it) {
        edge = NAVI_POOL_NEW_CLASS(_pool, Edge, _logger.logger, _pool);
        if (!edge->init(*this, inputDef)) {
            NAVI_POOL_DELETE_CLASS(edge);
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

void LocalSubGraph::terminate(ErrorCode ec) {
    for (const auto &node : _nodeVec) {
        node->terminate(ec);
    }
}

bool LocalSubGraph::addResourceNode() {
    try {
        GraphBuilder builder(_subGraphDef);
        MultiLayerResourceMap multiLayerResourceMap;
        collectResourceMap(multiLayerResourceMap);
        const auto &subGraph = *_subGraphDef;
        auto graphId = builder.currentSubGraph();
        auto nodeCount = subGraph.nodes_size();
        for (auto i = 0; i < nodeCount; i++) {
            const auto &node = subGraph.nodes(i);
            const auto &nodeName = node.name();
            const auto &kernelName = node.kernel_name();
            auto kernelCreator = _biz->getKernelCreator(kernelName);
            if (!kernelCreator) {
                NAVI_LOG(
                    ERROR, "kernel creator not found for kernel [%s], node [%s]", kernelName.c_str(), nodeName.c_str());
                setErrorCode(EC_CREATE_KERNEL);
                return false;
            }
            auto dependResourceMapPtr = _biz->getKernelDependResourceMap(kernelName);
            if (!dependResourceMapPtr) {
                NAVI_LOG(ERROR, "get kernel depend resource failed, kernel [%s]", kernelName.c_str());
                setErrorCode(EC_CREATE_KERNEL);
                return false;
            }
            size_t id = 0;
            for (const auto &pair : *dependResourceMapPtr) {
                const auto &resource = pair.first;
                _biz->addResourceGraph(resource, multiLayerResourceMap, builder);
                auto nodeInputPort = CommonUtil::getGroupPortName(NODE_RESOURCE_INPUT_PORT, id++);
                builder.linkInternal(graphId, resource, RESOURCE_CREATE_OUTPUT, nodeName, nodeInputPort);
            }
        }
        return true;
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_LOG(ERROR, "add resource graph failed, msg [%s]", e.GetMessage().c_str());
        return false;
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

}
