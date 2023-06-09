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

#include "navi/common.h"
#include <autil/DataBuffer.h>

namespace navi {

class ScheduleInfoDef;

class ScheduleInfo {
public:
    ScheduleInfo()
        : enqueueTime(0)
        , dequeueTime(0)
        , runningThread(0)
        , schedTid(0)
        , signalTid(0)
        , processTid(0)
        , threadCounter(0)
        , threadWaitCounter(0)
        , queueSize(0)
    {
    }
public:
    void fillProto(ScheduleInfoDef *info) const;
    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(enqueueTime);
        dataBuffer.write(dequeueTime);
        dataBuffer.write(runningThread);
        dataBuffer.write(schedTid);
        dataBuffer.write(signalTid);
        dataBuffer.write(processTid);
        dataBuffer.write(threadCounter);
        dataBuffer.write(threadWaitCounter);
        dataBuffer.write(queueSize);
    }
    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(enqueueTime);
        dataBuffer.read(dequeueTime);
        dataBuffer.read(runningThread);
        dataBuffer.read(schedTid);
        dataBuffer.read(signalTid);
        dataBuffer.read(processTid);
        dataBuffer.read(threadCounter);
        dataBuffer.read(threadWaitCounter);
        dataBuffer.read(queueSize);
    }
public:
    int64_t enqueueTime;
    int64_t dequeueTime;
    int64_t runningThread;
    int32_t schedTid;
    int32_t signalTid;
    int32_t processTid;
    int64_t threadCounter;
    int64_t threadWaitCounter;
    size_t queueSize;
};

}
