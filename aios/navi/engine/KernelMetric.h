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
    void fillProto(KernelComputeEventDef *event,
                   std::unordered_map<uint64_t, uint32_t> &addrMap) const;
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

class KernelMetric;
NAVI_TYPEDEF_PTR(KernelMetric);

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
    int64_t scheduleCount() const;
    int64_t tryScheduleCount() const;
    int64_t queueLatencyUs() const;
    int64_t totalLatencyUs() const;
    inline int64_t getMicroSecond(const timeval &val) const {
        return val.tv_sec * 1000000 + val.tv_usec;
    }
    KernelMetricPtr cloneAndClearEvent();
    std::string toString() const;
private:
    void incScheduleCount();
    void incTryScheduleCount();
    void reportTime(ErrorCode ec, int32_t type, NaviPartId partId,
                    int64_t beginTime, int64_t endTime,
                    const rusage &usageBegin, const rusage &usageEnd,
                    const ScheduleInfo &schedInfo,
                    const NaviPerfResultPtr &perfResult, size_t poolMallocCount,
                    size_t poolMallocSize);
    void reportTime(ErrorCode ec, int64_t beginTime, int64_t endTime,
                    const ScheduleInfo &schedInfo)
    {
        if (EC_NONE == _ec) {
            _ec = ec;
        }
        _queueLatencyNs += beginTime - schedInfo.enqueueTime;
        _computeLatencyNs += endTime - beginTime;
    }
    void fillProto(KernelMetricDef *metric,
                   std::unordered_map<uint64_t, uint32_t> &addrMap) const;
private:
    friend class Node;
    friend class ComputeScope;
    friend class GraphMetric;
private:
    GraphId _graphId;
    std::string _node;
    std::string _kernel;

    ErrorCode _ec; // need_restore
    int64_t _scheduleCount;
    int64_t _tryScheduleCount;
    int64_t _queueLatencyNs;
    int64_t _computeLatencyNs;
    mutable autil::ThreadMutex _mutex;
    std::shared_ptr<std::list<KernelComputeEvent>> _eventList; // need_restore
};

}
