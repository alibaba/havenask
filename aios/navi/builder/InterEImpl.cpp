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
#include "navi/builder/InterEImpl.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/builder/PImpl.h"
#include "navi/builder/SingleE.h"
#include "navi/builder/NImplBase.h"

namespace navi {

InterEImpl::InterEImpl() {
}

InterEImpl::~InterEImpl() {
    for (const auto &pair : _edgeMap) {
        delete pair.second;
    }
}

SingleE *InterEImpl::addDownStream(P to) {
    fillScopeBorder(to);
    to._impl->setConnected(to._index, _from);
    auto key = getEdgeKey(to);
    auto it = _edgeMap.find(key);
    if (_edgeMap.end() != it) {
        return it->second;
    }
    const auto &fromNode = _from._impl->getNodeName();
    const auto &toNode = to._impl->getNodeName();
    auto toPort = getPortName(to);
    auto edge = _builder->linkInternal(_fromGraphId, fromNode, _fromPort, toNode, toPort);
    auto singleE = new SingleE(edge);
    _edgeMap.emplace(key, singleE);
    return singleE;
}

N InterEImpl::split(const std::string &splitKernel) {
    if (_split) {
        return N(_split);
    }
    auto n = _builder->nodeInner(_fromGraphId, "", NT_FAKE_SPLIT);
    _split = n._impl;
    return n;
}

N InterEImpl::merge(const std::string &mergeKernel) {
    if (_merge) {
        return N(_merge);
    }
    auto n = _builder->nodeInner(_fromGraphId, "", NT_FAKE_MERGE);
    _merge = n._impl;
    return n;
}

void InterEImpl::fillScopeBorder(P to) const {
    auto fromNode = _from._impl->getNode();
    auto toNode = to._impl->getNode();
    if (fromNode->getScope() != toNode->getScope()) {
        fromNode->initScopeBorder(IOT_OUTPUT);
        toNode->initScopeBorder(IOT_INPUT);
    }
}

}

