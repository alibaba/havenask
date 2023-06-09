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
#include "navi/engine/PortDataQueue.h"

#include "navi/engine/Port.h"
#include "navi/log/NaviLogger.h"
#include "navi/util/CommonUtil.h"

namespace navi {

PortDataQueue::PortDataQueue()
    : dataCount(0)
    , totalDataCount(0)
    , eofMap(nullptr)
{
}

PortDataQueue::~PortDataQueue() {
    ReadyBitMap::freeReadyBitMap(eofMap);
}

void PortDataQueue::init(autil::mem_pool::Pool *pool, const PartInfo &eofPartInfo) {
    eofMap = eofPartInfo.createReadyBitMap(pool);
    eofMap->setFinish(false);
}

PortDataQueues::PortDataQueues()
    : _partQueues(nullptr)
    , _upStream(nullptr)
    , _downStream(nullptr)
    , _storeType(PST_UNKNOWN)
    , _queueType(PQT_UNKNOWN)
{
}

PortDataQueues::~PortDataQueues() {
    if (_partQueues) {
        for (NaviPartId partId = 0; partId < _partInfo.getFullPartCount(); partId++) {
            POOL_DELETE_CLASS(&_partQueues[partId]);
        }
        free(_partQueues);
        _partQueues = nullptr;
    }
}

bool PortDataQueues::init(const std::shared_ptr<NaviLogger> &logger,
                          autil::mem_pool::Pool *pool,
                          const PartInfo &partInfo,
                          const PartInfo &eofPartInfo,
                          Port *upStream,
                          Port *downStream,
                          PortStoreType storeType,
                          PortQueueType queueType)
{
    assert(partInfo.getFullPartCount() > 0 && "full part count must be non-zero");
    // TODO queues size = UsedPartCount?
    size_t allocSize = sizeof(PortDataQueue) * partInfo.getFullPartCount();
    auto mem = (PortDataQueue *)malloc(allocSize);
    for (NaviPartId partId = 0; partId < partInfo.getFullPartCount(); partId++) {
        auto partQueue = &mem[partId];
        new (partQueue) PortDataQueue();
        partQueue->init(pool, eofPartInfo);
    }
    _logger.object = this;
    _logger.logger = logger;
    _logger.addPrefix(
        "%s %s %p->%p %d %d", CommonUtil::getPortQueueType(queueType),
        CommonUtil::getStoreType(storeType),
        upStream, downStream,
        partInfo.getFullPartCount(), eofPartInfo.getFullPartCount());
    _partInfo = partInfo;
    _eofPartInfo = eofPartInfo;
    _partQueues = mem;
    _upStream = upStream;
    _downStream = downStream;
    _storeType = storeType;
    _queueType = queueType;
    NAVI_LOG(SCHEDULE2, "port data queues init, partInfo [%s] eofPartInfo [%s] "
             "storeType [%s] queueType [%s]",
             partInfo.ShortDebugString().c_str(), eofPartInfo.ShortDebugString().c_str(),
             CommonUtil::getStoreType(storeType),
             CommonUtil::getPortQueueType(queueType));
    return true;
}

PortStoreType PortDataQueues::getStoreType() const {
    return _storeType;
}

ErrorCode PortDataQueues::doSetEof(NaviPartId toPartId, NaviPartId fromPartId) {
    bool retEof;
    if (PST_DATA == _storeType) {
        return push(toPartId, fromPartId, DataPtr(), true, retEof);
    } else {
        return pushSerialize(toPartId, fromPartId, (NaviPortData *)nullptr,
                             true, retEof);
    }
}

bool PortDataQueues::checkPartId(NaviPartId toPartId,
                                 NaviPartId fromPartId) const
{
    auto fullPartCount = _partInfo.getFullPartCount();
    auto eofPartCount = _eofPartInfo.getFullPartCount();
    auto ret = toPartId < fullPartCount && fromPartId < eofPartCount;
    if (!ret) {
        NAVI_LOG(ERROR,
                 "partId overflow, toPartId: %d, fromPartId: %d, partCount: %d, "
                 "eofPartCount: %d",
                 toPartId, fromPartId, fullPartCount, eofPartCount);
    }
    return ret;
}

int64_t PortDataQueues::size(NaviPartId toPartId) const {
    PortDataQueue &portDataQueue = _partQueues[toPartId];
    return portDataQueue.dataCount;
}

bool PortDataQueues::convertPartId(PortQueueType queueType, NaviPartId to,
                                   NaviPartId from, NaviPartId &toBitMap,
                                   NaviPartId &fromBitMap,
                                   NaviPartId &toCallBack,
                                   NaviPartId &fromCallBack)
{
    switch (queueType) {
    case PQT_LOCAL_REMOTE_SERVER_1:
    case PQT_LOCAL_REMOTE_CLIENT_2:
    case PQT_LOCAL_LOCAL_3:
        fromBitMap = 0;
        toBitMap = to;
        fromCallBack = to;
        toCallBack = to;
        break;
    case PQT_REMOTE_LOCAL_SERVER_4:
        fromBitMap = 0;
        toBitMap = from;
        fromCallBack = 0;
        toCallBack = from;
        break;
    case PQT_REMOTE_LOCAL_CLIENT_5:
        fromBitMap = 0;
        toBitMap = from;
        fromCallBack = from;
        toCallBack = from;
        break;
    case PQT_REMOTE_REMOTE_CLIENT_6:
        fromBitMap = from;
        toBitMap = to;
        fromCallBack = from;
        toCallBack = to;
        break;
    case PQT_UNKNOWN:
    default:
        assert(false);
        fromBitMap = from;
        toBitMap = to;
        fromCallBack = from;
        toCallBack = to;
        return false;
    }
    return true;
}

ErrorCode PortDataQueues::pushSerialize(NaviPartId toPartId,
                                        NaviPartId fromPartId,
                                        NaviPortData *data,
                                        bool eof,
                                        bool &retEof)
{
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
        NAVI_LOG(ERROR, "partId overflow, input toPartId: %d, "
                        "input fromPartId: %d, eof: %d",
                 toPartId, fromPartId, eof);
        return EC_PART_ID_OVERFLOW;
    }
    assert(_partInfo.isUsed(toBitMap));
    assert(_eofPartInfo.isUsed(fromBitMap));

    _upStream->setCallback(fromCallBack, eof);
    {
        auto &portDataQueue = _partQueues[toBitMap];
        autil::ScopedLock lock(portDataQueue.lock);
        if (portDataQueue.eofMap->isFinish(fromBitMap)) {
            return EC_DATA_AFTER_EOF;
        }
        portDataQueue.eofMap->setFinish(fromBitMap, eof);
        retEof = portDataQueue.eofMap->isFinish();
        NaviPortData temp;
        if (data) {
            temp.Swap(data);
        } else {
            if (!SerializeData::serialize(_logger, toPartId, fromPartId,
                                          nullptr, true, nullptr, temp))
            {
                return EC_SERIALIZE;
            }
        }
        portDataQueue.serializeDataList.emplace_back(std::move(temp));
        portDataQueue.dataCount++;
        portDataQueue.totalDataCount++;
        NAVI_LOG(SCHEDULE2,
                 "upstream [%p,%s], downstream [%p,%s], portId: %d, "
                 "portKey: %s, from: %d, to: %d, eof: %d, "
                 "portEof: %d, dataCount: %ld, totalDataCount: %ld, retEof: %d",
                 _upStream, typeid(*_upStream).name(),
                 _downStream, typeid(*_downStream).name(),
                 _upStream->getPortId(), _upStream->getPortKey().c_str(),
                 fromPartId, toPartId, eof, portDataQueue.eofMap->isFinish(),
                 portDataQueue.dataCount, portDataQueue.totalDataCount, retEof);
    }
    _downStream->incBorderMessageCount(toBitMap);
    _downStream->setCallback(toCallBack, eof);
    return EC_NONE;
}

ErrorCode PortDataQueues::popSerialize(NaviPartId toPartId, NaviPortData *data,
                                       bool &eof)
{
    if (!checkPartId(toPartId, 0)) {
        NAVI_LOG(ERROR, "partId overflow, toPartId: %d", toPartId);
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
        auto &dataList = portDataQueue.serializeDataList;
        assert(!dataList.empty());
        auto &listData = dataList.front();
        assert(data);
        data->Swap(&listData);
        dataList.pop_front();
    }
    _downStream->decBorderMessageCount(toPartId);
    _downStream->getCallback(toPartId, eof);
    _upStream->getCallback(toPartId, eof);
    return EC_NONE;
}

std::ostream& operator<<(std::ostream& os, const PortDataQueues& portDataQueues) {
    return os << &portDataQueues << ","
              << typeid(portDataQueues).name() << ","
              << portDataQueues._partInfo.ShortDebugString() << ","
              << portDataQueues._eofPartInfo.ShortDebugString();
}

}
