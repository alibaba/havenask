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
#include "navi/engine/LocalOutputPort.h"
#include "navi/engine/Node.h"
#include "navi/engine/TypeContext.h"
#include "navi/util/ReadyBitMap.h"

namespace navi {

LocalOutputPort::LocalOutputPort(const EdgeDef &def, SubGraphBorder *border)
    : LocalPortBase(def.input(), def.output(), IOT_OUTPUT, border)
{
    _logger.addPrefix("LocalOutputPort");
}

LocalOutputPort::~LocalOutputPort() {
}

void LocalOutputPort::setCallback(NaviPartId partId, bool eof) {
    NAVI_LOG(SCHEDULE2, "set callback part [%d] eof [%d] node [%p] nodeReadyMap [%p]",
             partId, eof, _node, _nodeReadyMap);
    if (!_node || !_nodeReadyMap) {
        return;
    }
    {
        autil::ScopedLock lock(_lock);
        // bool ready = !queueFull(queueSize);
        bool ready = true;
        bool finish = eof;
        _nodeReadyMap->setReady(partId, ready);
        _nodeReadyMap->setFinish(partId, finish);
        _node->incNodeSnapshot();
    }
    scheduleNode();
}

void LocalOutputPort::getCallback(NaviPartId partId, bool eof) {
    NAVI_LOG(SCHEDULE2, "get callback part [%d] eof [%d] node [%p] nodeReadyMap [%p]",
             partId, eof, _node, _nodeReadyMap);
    if (!_node || !_nodeReadyMap) {
        return;
    }
    {
        autil::ScopedLock lock(_lock);
        bool ready = true;
        _nodeReadyMap->setReady(partId, ready);
        _node->incNodeSnapshot();
    }
    scheduleNode();
}

}
