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
#include "navi/builder/BorderEImpl.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/builder/PImpl.h"
#include "navi/builder/NImplBase.h"

namespace navi {

BorderEImpl::BorderEImpl()
    : _fromBorder(nullptr)
    , _toBorder(nullptr)
{
}

BorderEImpl::~BorderEImpl() {
}

SingleE *BorderEImpl::addDownStream(P to) {
    if (!_fromBorder) {
        return addWithBorder(to);
    } else {
        return addWithoutBorder(to);
    }
}

SingleE *BorderEImpl::addWithBorder(P to) {
    const auto &fromNode = _from._impl->getNodeName();
    const auto &toNode = to._impl->getNodeName();
    auto toPort = getPortName(to);
    BorderId fromBorderId(IOT_OUTPUT, _fromGraphId, _toGraphId);
    BorderId toBorderId(IOT_INPUT, _toGraphId, _fromGraphId);
    _fromBorder = _builder->linkBorder(fromBorderId, fromNode, _fromPort, toNode, toPort);
    _toBorder = _builder->linkBorder(toBorderId, fromNode, _fromPort, toNode, toPort);
    if (!_fromBorder || !_toBorder) {
        return nullptr;
    }
    _fromBorder->set_peer_edge_id(_toBorder->edge_id());
    _toBorder->set_peer_edge_id(_fromBorder->edge_id());
    addSplit();
    return addMerge(to);
}

void BorderEImpl::addSplit() {
    auto nodeName = "split:" + autil::StringUtil::toString(_fromBorder->edge_id()) + ":" +
                    autil::StringUtil::toString(_fromBorder->peer_edge_id());
    _fromBorder->set_node(nodeName);
    auto fromNodeType = _from._impl->getNode()->getNodeType();
    auto fromTerminator = (NT_SCOPE_TERMINATOR == fromNodeType);
    auto nodeType = fromTerminator ? NT_SCOPE_TERMINATOR_SPLIT : NT_SPLIT;
    auto n = _builder->nodeInner(_fromGraphId, nodeName, nodeType);
    n.kernel(DEFAULT_SPLIT_KERNEL);
    _split = n._impl;
    if (!fromTerminator) {
        _split->initScopeBorder(IOT_OUTPUT);
    }
    _from.to(n.in(PORT_KERNEL_INPUT_PORT));
}

SingleE *BorderEImpl::addMerge(P to) {
    auto nodeName = "merge:" + autil::StringUtil::toString(_toBorder->edge_id()) + ":" +
                    autil::StringUtil::toString(_toBorder->peer_edge_id());
    _toBorder->set_node(nodeName);
    auto n = _builder->nodeInner(_toGraphId, nodeName, NT_MERGE);
    n.kernel(DEFAULT_MERGE_KERNEL);
    _merge = n._impl;
    _merge->initScopeBorder(IOT_INPUT);
    auto mergePort = n.out(PORT_KERNEL_OUTPUT_PORT);
    mergePort._impl->setRealPort(_from);
    auto e = to.from(mergePort);
    return e._singleE;
}

SingleE *BorderEImpl::addWithoutBorder(P to) {
    assert(_merge);
    auto from = N(_merge);
    auto e = to.from(from.out(PORT_KERNEL_OUTPUT_PORT));
    return e._singleE;
}

N BorderEImpl::split(const std::string &splitKernel) {
    assert(_split);
    auto n = N(_split);
    n.kernel(splitKernel);
    return n;
}

N BorderEImpl::merge(const std::string &mergeKernel) {
    assert(_merge);
    auto n = N(_merge);
    n.kernel(mergeKernel);
    return n;
}

std::ostream &operator<<(std::ostream &os, const BorderEImpl &edge) {
    return os;
}

}

