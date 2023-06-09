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
#include "navi/engine/GraphDomainFork.h"
#include "navi/engine/Edge.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/Node.h"
#include "navi/util/CommonUtil.h"

namespace navi {

GraphDomainFork::GraphDomainFork(Graph *graph, const BizPtr &biz, Node *node,
                                 NaviPartId partCount, NaviPartId partId)
    : GraphDomain(graph, GDT_FORK)
    , _biz(biz)
    , _node(node)
    , _partCount(partCount)
    , _partId(partId)
{
    _logger.addPrefix("node [%s]", node->getName().c_str());
}

GraphDomainFork::~GraphDomainFork() {
}

// TODO init border info??
bool GraphDomainFork::postInit() {
    if (!initPortDataType()) {
        NAVI_LOG(ERROR, "init port data type failed");
        return false;
    }
    initPartCount();
    return true;
}

bool GraphDomainFork::run() {
    return true;
}

void GraphDomainFork::notifySend(NaviPartId partId) {
}

bool GraphDomainFork::initPortDataType() {
    auto localGraph = _node->getGraph();
    for (const auto &pair : _portMap) {
        auto port = pair.second;
        auto ioType = CommonUtil::getReverseIoType(port->getIoType());
        if (!localGraph->bindPortDataType(_node, port, ioType)) {
            return false;
        }
    }
    return true;
}

void GraphDomainFork::initPartCount() {
    for (auto subGraphDef : _subGraphs) {
        auto location = subGraphDef->mutable_location();
        if (INVALID_NAVI_PART_COUNT == location->part_info().part_count()) {
            location->mutable_part_info()->set_part_count(_partCount);
            location->set_this_part_id(_partId);
        }
        NAVI_LOG(SCHEDULE1, "graph: %d, location: %s", subGraphDef->graph_id(),
                 location->DebugString().c_str());
    }
}

bool GraphDomainFork::bindGraphEdgeList(
    const std::unordered_map<std::string, Edge *> &edges)
{
    for (const auto &pair : edges) {
        auto inputDef = pair.second->getInputDef();
        if (NAVI_FORK_NODE == inputDef->node_name()) {
            _inputEdgeMap.emplace(inputDef->port_name(), pair.second);
        } else {
            const auto &outputSlots = pair.second->getOutputSlots();
            auto slotCount = outputSlots.size();
            for (IndexType slotId = 0; slotId < slotCount; slotId++) {
                const auto &slot = outputSlots[slotId];
                auto outputPortDef = slot.outputDef;
                if (NAVI_FORK_NODE != outputPortDef->node_name()) {
                    continue;
                }
                const auto &portName = outputPortDef->port_name();
                auto ret = _outputEdgeMap.emplace(
                    portName, EdgeOutputInfo(pair.second, slotId));
                if (!ret.second) {
                    NAVI_LOG(ERROR,
                            "multiple fork output link to the same output port: %s",
                            portName.c_str());
                    return false;
                }
            }
        }
    }
    return true;
}

Edge *GraphDomainFork::getInputPeer(const std::string &portName) const {
    auto it = _inputEdgeMap.find(portName);
    if (_inputEdgeMap.end() != it) {
        return it->second;
    } else {
        return nullptr;
    }
}

EdgeOutputInfo GraphDomainFork::getOutputPeer(const std::string &portName) const {
    auto it = _outputEdgeMap.find(portName);
    if (_outputEdgeMap.end() != it) {
        return it->second;
    } else {
        return {};
    }
}

void GraphDomainFork::flushData() const {
    for (const auto &pair : _inputEdgeMap) {
        pair.second->flushData();
    }
}

bool GraphDomainFork::checkBind() const {
    for (const auto &pair : _inputEdgeMap) {
        auto inputEdge = pair.second;
        if (!inputEdge->hasUpStream()) {
            NAVI_LOG(ERROR, "fork input port [%s] not exist",
                     pair.first.c_str());
            return false;
        }
    }
    for (const auto &pair : _outputEdgeMap) {
        auto outputInfo = pair.second;
        if (!outputInfo.hasDownStream() && !outputInfo.finished()) {
            NAVI_LOG(ERROR, "fork output port [%s] not exist",
                     pair.first.c_str());
            return false;
        }
    }
    return true;
}

Edge *GraphDomainFork::getEdge(
        const std::unordered_map<std::string, Edge *> &edgeMap,
        const std::string &port) const
{
    auto it = edgeMap.find(port);
    if (edgeMap.end() == it) {
        return nullptr;
    } else {
        return it->second;
    }
}

BizPtr GraphDomainFork::getBiz() const {
    return _biz;
}

}
