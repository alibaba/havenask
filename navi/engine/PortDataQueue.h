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
#pragma once

#include <list>
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/PartInfo.h"
#include "navi/engine/SerializeData.h"
#include "navi/proto/NaviStream.pb.h"
#include "navi/util/ReadyBitMap.h"


namespace navi {

class NaviLogger;
class PartInfo;

class PortDataQueue {
public:
    PortDataQueue();
    ~PortDataQueue();
public:
    void init(autil::mem_pool::Pool *pool, const PartInfo &eofPartInfo);
public:
    autil::ThreadMutex lock;
    int64_t dataCount;
    int64_t totalDataCount;
    std::list<DataPtr> dataList;
    std::list<NaviPortData> serializeDataList;
    ReadyBitMap *eofMap;
};

class Port;

class PortDataQueues {
public:
    PortDataQueues();
    virtual ~PortDataQueues();
public:
    bool init(const std::shared_ptr<NaviLogger> &logger,
              autil::mem_pool::Pool *pool, const PartInfo& partInfo,
              const PartInfo& eofPartInfo, Port *upStream, Port *downStream,
              PortStoreType storeType, PortQueueType queueType);
    PortStoreType getStoreType() const;
    int64_t size(NaviPartId toPartId) const;
public:
    virtual ErrorCode setEof(NaviPartId fromPartId) = 0;
    virtual ErrorCode push(NaviPartId toPartId, NaviPartId fromPartId,
                           const DataPtr &data, bool eof, bool &retEof) = 0;
    virtual ErrorCode pop(NaviPartId toPartId, DataPtr &data, bool &eof) = 0;
    ErrorCode pushSerialize(NaviPartId toPartId, NaviPartId fromPartId,
                            NaviPortData *data, bool eof, bool &retEof);
    ErrorCode popSerialize(NaviPartId toPartId, NaviPortData *data, bool &eof);
protected:
    ErrorCode doSetEof(NaviPartId toPartId, NaviPartId fromPartId);
    bool checkPartId(NaviPartId toPartId, NaviPartId fromPartId) const;
public:
    static bool convertPartId(PortQueueType queueType, NaviPartId to,
                              NaviPartId from, NaviPartId &toBitMap,
                              NaviPartId &fromBitMap, NaviPartId &toCallBack,
                              NaviPartId &fromCallBack);
    friend std::ostream& operator<<(std::ostream& os, const PortDataQueues& portDataQueues);
protected:
    DECLARE_LOGGER();
    PartInfo _partInfo;
    PartInfo _eofPartInfo;
    PortDataQueue *_partQueues;
    Port *_upStream;
    Port *_downStream;
    PortStoreType _storeType;
    PortQueueType _queueType;
};

}
