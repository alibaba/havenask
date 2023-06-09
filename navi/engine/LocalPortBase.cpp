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
#include "navi/engine/LocalPortBase.h"
#include "navi/engine/Node.h"
#include "navi/util/ReadyBitMap.h"

namespace navi {

LocalPortBase::LocalPortBase(const NodePortDef &nodePort,
                             const NodePortDef &peerPort,
                             IoType ioType,
                             SubGraphBorder *border)
    : Port(nodePort, peerPort, ioType, PST_DATA, border)
    , _node(nullptr)
    , _nodeReadyMap(nullptr)
{
}

LocalPortBase::~LocalPortBase() {
}

void LocalPortBase::setNode(Node *node, ReadyBitMap *nodeReadyMap) {
    _node = node;
    _nodeReadyMap = nodeReadyMap;
    NAVI_LOG(SCHEDULE2, "set node [%p] nodeReadyMap [%s]",
             node, autil::StringUtil::toString(*nodeReadyMap).c_str());
}

void LocalPortBase::scheduleNode() const {
    if (_node) {
        _node->schedule(_node);
    }
}

}
