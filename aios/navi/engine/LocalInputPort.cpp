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
#include "navi/engine/LocalInputPort.h"
#include "navi/engine/Node.h"
#include "navi/engine/SubGraphBorder.h"
#include "navi/engine/TypeContext.h"
#include "navi/util/ReadyBitMap.h"

namespace navi {

LocalInputPort::LocalInputPort(const EdgeDef &def,
                               SubGraphBorder *border)
    : LocalPortBase(def.output(), def.input(), IOT_INPUT, border)
{
    _logger.addPrefix("LocalInputPort");
}

LocalInputPort::~LocalInputPort() {
}

void LocalInputPort::setCallback(NaviPartId partId, bool eof) {
    NAVI_LOG(SCHEDULE2, "set callback part [%d] eof [%d] node [%p] nodeReadyMap [%p]",
             partId, eof, _node, _nodeReadyMap);
    _border->notifySend(partId);
    if (!_node || !_nodeReadyMap) {
        return;
    }
    doCallBack(partId, eof);
}

void LocalInputPort::getCallback(NaviPartId partId, bool eof) {
    NAVI_LOG(SCHEDULE2, "get callback part [%d] eof [%d] node [%p] nodeReadyMap [%p]",
             partId, eof, _node, _nodeReadyMap);
    if (!_node || !_nodeReadyMap) {
        return;
    }
    doCallBack(partId, eof);
}

void LocalInputPort::doCallBack(NaviPartId partId, bool eof) {
    {
        autil::ScopedLock lock(_lock);
        auto queueSize = getQueueSize(partId);
        bool ready = queueSize > 0;
        bool finish = eof && (queueSize == 0);
        NAVI_LOG(SCHEDULE2, "do callback part [%d] ready [%d] finish [%d] optional [%d]",
                 partId, ready, finish,
                 _nodeReadyMap->isOptional(partId));
        _nodeReadyMap->setReady(partId, ready);
        _nodeReadyMap->setFinish(partId, finish);
        _node->incNodeSnapshot();
    }
    NAVI_LOG(SCHEDULE2, "after set nodeReadyMap [%s]",
             autil::StringUtil::toString(*_nodeReadyMap).c_str());
    scheduleNode();
}

}
