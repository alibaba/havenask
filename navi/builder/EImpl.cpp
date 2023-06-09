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
#include "navi/builder/EImpl.h"
#include "navi/builder/PImpl.h"
#include "navi/builder/InterEImpl.h"
#include "navi/builder/BorderEImpl.h"
#include "navi/builder/SingleE.h"

namespace navi {

EImpl::EImpl()
    : _builder(nullptr)
    , _fromGraphId(INVALID_GRAPH_ID)
    , _from(nullptr, INVALID_INDEX)
    , _toGraphId(INVALID_GRAPH_ID)
    , _split(nullptr)
    , _merge(nullptr)
{
}

EImpl::~EImpl() {
}

bool EImpl::init(GraphBuilder *builder, GraphId peerGraphId, P from) {
    _builder = builder;
    _fromGraphId = from._impl->graphId();
    _from = from;
    _fromPort = getPortName(from);
    _toGraphId = peerGraphId;
    return true;
}

EImpl *EImpl::createEdge(GraphBuilder *builder, GraphId peerGraphId, P from) {
    std::unique_ptr<EImpl> e;
    if (from._impl->graphId() == peerGraphId) {
        e.reset(new InterEImpl());
    } else {
        e.reset(new BorderEImpl());
    }
    if (!e->init(builder, peerGraphId, from)) {
        return nullptr;
    }
    return e.release();
}

std::string EImpl::getPortName(P p) {
    auto index = p._index;
    const auto &port = p._impl->getPort();
    if (INVALID_INDEX == index) {
        return port;
    }
    return port + GROUP_PORT_SEPERATOR + autil::StringUtil::toString(index);
}

}

