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
#include "navi/engine/NormalPortQueue.h"
#include "navi/engine/Port.h"

namespace navi {

NormalPortQueue::NormalPortQueue() {
    _logger.addPrefix("normal");
}

NormalPortQueue::~NormalPortQueue() {}

ErrorCode NormalPortQueue::setEof(NaviPartId fromPartId) {
    return doSetEof(0, fromPartId);
}

ErrorCode NormalPortQueue::push(NaviPartId toPartId, NaviPartId fromPartId,
                                const DataPtr &data, bool eof, bool &retEof)
{
    assert(_partInfo.isUsed(toPartId));

    NaviPartId toBitMap;
    NaviPartId fromBitMap;
    NaviPartId toCallBack;
    NaviPartId fromCallBack;
    if (!convertPartId(_queueType, toPartId, fromPartId, toBitMap, fromBitMap,
                       toCallBack, fromCallBack))
    {
        return EC_UNKNOWN;
    }
    if (!checkPartId(toBitMap, fromBitMap)) {
        return EC_PART_ID_OVERFLOW;
    }
    {
        auto &portDataQueue = _partQueues[toBitMap];
        autil::ScopedLock lock(portDataQueue.lock);
        if (portDataQueue.eofMap->isFinish(fromBitMap)) {
            return EC_DATA_AFTER_EOF;
        }
        portDataQueue.eofMap->setFinish(fromBitMap, eof);
        retEof = portDataQueue.eofMap->isFinish();
        portDataQueue.dataList.push_back(data);
        portDataQueue.dataCount++;
        portDataQueue.totalDataCount++;
    }
    _downStream->incBorderMessageCount(toBitMap);
    _downStream->setCallback(toCallBack, retEof);
    _upStream->setCallback(fromCallBack, eof);
    return EC_NONE;
}

ErrorCode NormalPortQueue::pop(NaviPartId toPartId, DataPtr &data, bool &eof) {
    if (!checkPartId(toPartId, 0)) {
        return EC_PART_ID_OVERFLOW;
    }
    {
        auto &portDataQueue = _partQueues[toPartId];
        autil::ScopedLock lock(portDataQueue.lock);
        if (0 == portDataQueue.dataCount) {
            eof = portDataQueue.eofMap->isFinish();
            return EC_NO_DATA;
        }
        portDataQueue.dataCount--;
        eof = portDataQueue.eofMap->isFinish() && portDataQueue.dataCount == 0;
        auto &dataList = portDataQueue.dataList;
        assert(!dataList.empty());
        data = dataList.front();
        dataList.pop_front();
    }
    _downStream->decBorderMessageCount(toPartId);
    _downStream->getCallback(toPartId, eof);
    _upStream->getCallback(toPartId, eof);
    return EC_NONE;
}

}
