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
#include "navi/builder/PImpl.h"
#include "navi/builder/EImpl.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/builder/GraphDesc.h"
#include "navi/builder/NImpl.h"

namespace navi {

PImpl::PImpl(const NImplBase *node, const std::string &port, bool input)
    : _node(node)
    , _nodeName(node->name())
    , _port(port)
    , _input(input)
    , _group(false)
    , _currentIndex(0)
    , _realPort(nullptr, INVALID_INDEX)
{
}

PImpl::~PImpl() {
    for (const auto &pair : _edgeMap) {
        delete pair.second;
    }
}

GraphId PImpl::graphId() const {
    return _node->graphId();
}

const NImplBase *PImpl::getNode() const {
    return _node;
}

const std::string &PImpl::getNodeName() const {
    return _node->name();
}

const std::string &PImpl::getPort() const {
    return _port;
}

E PImpl::from(IndexType thisIndex, P peer) {
    return peer._impl->to(peer._index, P(this, thisIndex));
}

E PImpl::to(IndexType thisIndex, P peer) {
    auto peerImpl = peer._impl;
    if (_input) {
        throw autil::legacy::ExceptionBase(
            "error direction, can not call \'to\' on input port, use \'from\' instead, port info: " +
            autil::StringUtil::toString(P(this, thisIndex)));
    }
    auto peerGraphId = peerImpl->graphId();
    auto key = getEdgeKey(thisIndex, peerGraphId);
    EImpl *e = nullptr;
    auto it = _edgeMap.find(key);
    if (_edgeMap.end() != it) {
        e = it->second;
    } else {
        e = EImpl::createEdge(_node->builder(), peerGraphId, P(this, thisIndex));
        if (!e) {
            throw autil::legacy::ExceptionBase(
                "can't create edge, from: " + autil::StringUtil::toString(P(this, thisIndex)) +
                ", to: " + autil::StringUtil::toString(peer));
        }
        _edgeMap.emplace(key, e);
    }
    auto singleE = e->addDownStream(peer);
    if (!singleE) {
        throw autil::legacy::ExceptionBase(
            "can't add down stream edge, from: " + autil::StringUtil::toString(P(this, thisIndex)) +
            ", to: " + autil::StringUtil::toString(peer));
    }
    return E(e, singleE);
}

E PImpl::fromForkNodeInput(IndexType thisIndex, const std::string &input, IndexType index) {
    auto n = _node->builder()->getParentNode();
    auto other = n.out(input);
    P p(this, thisIndex);
    if (INVALID_INDEX != index) {
        return p.from(other.asGroup().index(index));
    } else {
        return p.from(other);
    }
}

E PImpl::toForkNodeOutput(IndexType thisIndex, const std::string &output, IndexType index) {
    auto n = _node->builder()->getParentNode();
    auto other = n.in(output);
    P p(this, thisIndex);
    if (INVALID_INDEX != index) {
        return other.asGroup().index(index).from(p);
    } else {
        return other.from(p);
    }
}

E PImpl::asGraphOutput(IndexType thisIndex, const std::string &name) {
    auto n = _node->builder()->getOutputNode(name);
    return n.in("").from(P(this, thisIndex));
}

void PImpl::setConnected(IndexType inputIndex, P from) {
    if (!_group && (inputIndex != INVALID_INDEX)) {
        throw autil::legacy::ExceptionBase("invalid port index, port is not in group mode" +
                                           autil::StringUtil::toString(P(this, inputIndex)));
    }
    auto it = _connectedInputSet.find(inputIndex);
    if (_connectedInputSet.end() != it) {
        if (INVALID_INDEX == inputIndex && 1u == _connectedInputSet.size()) {
            return;
        }
        throw autil::legacy::ExceptionBase(
            "error, double connection to: " + autil::StringUtil::toString(P(this, inputIndex)) +
            ", first: " + autil::StringUtil::toString(it->second) + ", second: " + autil::StringUtil::toString(from));
    }
    _connectedInputSet.emplace(inputIndex, from);
}

void PImpl::setRealPort(P real) {
    _realPort = real;
}

P PImpl::getRealPort() const {
    return _realPort;
}

P PImpl::asGroup() {
    if (hasConnected() && !_group) {
        throw autil::legacy::ExceptionBase("can't switch to group mode, port already connected, " +
                                           autil::StringUtil::toString(P(this, INVALID_INDEX)));
    }
    _group = true;
    return P(this, 0);
}

P PImpl::next(IndexType index) {
    if (!_group) {
        throw autil::legacy::ExceptionBase("not group mode port, call asGroup first, " +
                                           autil::StringUtil::toString(P(this, INVALID_INDEX)));
    }
    return P(this, index + 1);
}

P PImpl::index(IndexType index) {
    P p(this, index);
    if (INVALID_INDEX == index) {
        throw autil::legacy::ExceptionBase("invalid group index: " + autil::StringUtil::toString(index) + ", " +
                                           autil::StringUtil::toString(p));
    }
    if (!_group) {
        throw autil::legacy::ExceptionBase("not group mode port, call asGroup first, " +
                                           autil::StringUtil::toString(p));
    }
    return p;
}

P PImpl::autoNext() {
    if (!_group) {
        asGroup();
    }
    return P(this, _currentIndex++);
}

bool PImpl::hasConnected() const {
    return (!_edgeMap.empty()) || (!_connectedInputSet.empty());
}

}
