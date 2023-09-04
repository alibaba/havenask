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

#include "navi/engine/ScheduleInfo.h"
#include "navi/perf/NaviPerfResult.h"
#include <autil/DataBuffer.h>
#include <list>
#include <sys/resource.h>
#include <sys/time.h>

namespace navi {

class KernelMetricDef;
class KernelComputeEventDef;

struct KernelComputeEvent {
public:
    int64_t queueTime() const {
        return beginTime - scheduleInfo.enqueueTime;
    }
    int64_t computeTime() const {
        return endTime - beginTime;
    }
    void fillProto(KernelComputeEventDef *event,
                   std::unordered_map<uint64_t, uint32_t> &addrMap) const;
    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(type);
        dataBuffer.write(partId);
        dataBuffer.write(scheduleId);
        dataBuffer.write(scheduleInfo);
        dataBuffer.write(beginTime);
        dataBuffer.write(endTime);
        dataBuffer.write(utimeInUs);
        dataBuffer.write(stimeInUs);
        dataBuffer.write(nvcsw);
        dataBuffer.write(nivcsw);
        dataBuffer.write(minflt);
        dataBuffer.write(majflt);
        dataBuffer.write(poolMallocCount);
        dataBuffer.write(poolMallocSize);
    }
    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(type);
        dataBuffer.read(partId);
        dataBuffer.read(scheduleId);
        dataBuffer.read(scheduleInfo);
        dataBuffer.read(beginTime);
        dataBuffer.read(endTime);
        dataBuffer.read(utimeInUs);
        dataBuffer.read(stimeInUs);
        dataBuffer.read(nvcsw);
        dataBuffer.read(nivcsw);;
        dataBuffer.read(minflt);
        dataBuffer.read(majflt);
        dataBuffer.read(poolMallocCount);
        dataBuffer.read(poolMallocSize);
    }
public:
    int32_t type;
    NaviPartId partId;
    int64_t scheduleId;
    ScheduleInfo scheduleInfo;
    int64_t beginTime;
    int64_t endTime;
    int64_t utimeInUs;
    int64_t stimeInUs;
    int64_t nvcsw;
    int64_t nivcsw;
    int64_t minflt;
    int64_t majflt;
    int64_t poolMallocCount;
    int64_t poolMallocSize;
    NaviPerfResultPtr perfResult;
};

class KernelMetric
{
public:
    KernelMetric(GraphId graphId,
                 const std::string &node,
                 const std::string &kernel);
    KernelMetric();
    ~KernelMetric();
private:
    KernelMetric(const KernelMetric &);
    KernelMetric &operator=(const KernelMetric &);
public:
    void incScheduleCount();
    void incTryScheduleCount();
    void reportTime(int32_t type, NaviPartId partId, int64_t beginTime,
                    int64_t endTime, const rusage &usageBegin,
                    const rusage &usageEnd, const ScheduleInfo &schedInfo,
                    const NaviPerfResultPtr &perfResult, size_t poolMallocCount,
                    size_t poolMallocSize);
public:
    int64_t scheduleCount() const;
    int64_t tryScheduleCount() const;
    int64_t queueLatencyUs() const;
    int64_t totalLatencyUs() const;
    inline int64_t getMicroSecond(const timeval &val) const {
        return val.tv_sec * 1000000 + val.tv_usec;
    }
public:
    void fillProto(KernelMetricDef *metric,
                   std::unordered_map<uint64_t, uint32_t> &addrMap) const;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toString() const;
public:
    GraphId _graphId;
    std::string _node;
    std::string _kernel;

    int64_t _scheduleCount;
    int64_t _tryScheduleCount;
    int64_t _queueLatencyNs;
    int64_t _computeLatencyNs;
    std::list<KernelComputeEvent> _eventList;
};

}
