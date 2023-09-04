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
#include "navi/engine/KernelMetric.h"
#include "autil/StringUtil.h"
#include "navi/proto/GraphVis.pb.h"

namespace navi {

void KernelComputeEvent::fillProto(KernelComputeEventDef *event,
                                   std::unordered_map<uint64_t, uint32_t> &addrMap) const
{
    event->set_type((GraphEventType)type);
    event->set_part_id(partId);
    event->set_schedule_id(scheduleId);
    scheduleInfo.fillProto(event->mutable_schedinfo());
    event->set_begin_time_us(beginTime);
    event->set_end_time_us(endTime);
    event->set_utime_in_us(utimeInUs);
    event->set_stime_in_us(stimeInUs);
    event->set_nvcsw(nvcsw);
    event->set_nivcsw(nivcsw);
    event->set_minflt(minflt);
    event->set_majflt(majflt);
    event->set_pool_malloc_count(poolMallocCount);
    event->set_pool_malloc_size(poolMallocSize);
    if (perfResult) {
        perfResult->fillProto(event->mutable_perf(), addrMap);
    }
}

KernelMetric::KernelMetric(GraphId graphId,
                           const std::string &node,
                           const std::string &kernel)
    : _graphId(graphId)
    , _node(node)
    , _kernel(kernel)
    , _scheduleCount(0)
    , _tryScheduleCount(0)
    , _queueLatencyNs(0)
    , _computeLatencyNs(0)
{
}

KernelMetric::KernelMetric()
    : KernelMetric(INVALID_GRAPH_ID, "", "")
{
}

KernelMetric::~KernelMetric() {
}

void KernelMetric::incScheduleCount() {
    _scheduleCount++;
}

void KernelMetric::incTryScheduleCount() {
    _tryScheduleCount++;
}

int64_t KernelMetric::scheduleCount() const {
    return _scheduleCount;
}

int64_t KernelMetric::tryScheduleCount() const {
    return _tryScheduleCount;
}

void KernelMetric::reportTime(int32_t type,
                              NaviPartId partId,
                              int64_t beginTime,
                              int64_t endTime,
                              const rusage &usageBegin,
                              const rusage &usageEnd,
                              const ScheduleInfo &schedInfo,
                              const NaviPerfResultPtr &perfResult,
                              size_t poolMallocCount,
                              size_t poolMallocSize)
{
    KernelComputeEvent event;
    event.type = type;
    event.partId = partId;
    event.scheduleId = scheduleCount();
    event.scheduleInfo = schedInfo;
    event.beginTime = beginTime;
    event.endTime = endTime;
    event.utimeInUs =
        getMicroSecond(usageEnd.ru_utime) - getMicroSecond(usageBegin.ru_utime);
    event.stimeInUs =
        getMicroSecond(usageEnd.ru_stime) - getMicroSecond(usageBegin.ru_stime);
    event.nvcsw = usageEnd.ru_nvcsw - usageBegin.ru_nvcsw;
    event.nivcsw = usageEnd.ru_nivcsw - usageBegin.ru_nivcsw;
    event.minflt = usageEnd.ru_minflt - usageBegin.ru_minflt;
    event.majflt = usageEnd.ru_majflt - usageBegin.ru_majflt;
    event.poolMallocCount = poolMallocCount;
    event.poolMallocSize = poolMallocSize;
    event.perfResult = perfResult;
    _queueLatencyNs += event.queueTime();
    _computeLatencyNs += event.computeTime();
    _eventList.emplace_back(event);
}

int64_t KernelMetric::queueLatencyUs() const {
    return _queueLatencyNs / 1000;
}

int64_t KernelMetric::totalLatencyUs() const {
    return _computeLatencyNs / 1000;
}

void KernelMetric::fillProto(KernelMetricDef *metric,
                             std::unordered_map<uint64_t, uint32_t> &addrMap) const
{
    metric->set_graph_id(_graphId);
    metric->set_node(_node);
    metric->set_kernel(_kernel);
    metric->set_schedule_count(_scheduleCount);
    metric->set_try_schedule_count(_tryScheduleCount);
    metric->set_queue_latency_us(queueLatencyUs());
    metric->set_compute_latency_us(totalLatencyUs());

    for (const auto &event : _eventList) {
        event.fillProto(metric->add_events(), addrMap);
    }
}

void KernelMetric::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_graphId);
    dataBuffer.write(_node);
    dataBuffer.write(_kernel);
    dataBuffer.write(_scheduleCount);
    dataBuffer.write(_tryScheduleCount);
    dataBuffer.write(_queueLatencyNs);
    dataBuffer.write(_computeLatencyNs);

    dataBuffer.write(_eventList.size());
    for (const auto &event : _eventList) {
        dataBuffer.write(event);
    }
}

void KernelMetric::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_graphId);
    dataBuffer.read(_node);
    dataBuffer.read(_kernel);
    dataBuffer.read(_scheduleCount);
    dataBuffer.read(_tryScheduleCount);
    dataBuffer.read(_queueLatencyNs);
    dataBuffer.read(_computeLatencyNs);

    size_t eventSize = 0;
    dataBuffer.read(eventSize);
    for (size_t i = 0; i < eventSize; ++i) {
        KernelComputeEvent event;
        dataBuffer.read(event);
        _eventList.emplace_back(event);
    }
}

std::string KernelMetric::toString() const {
    std::string ret;
    ret += "graphId: " + autil::StringUtil::toString(_graphId)
           + ", node: " + autil::StringUtil::toString(_node)
           + ", kernel: " + autil::StringUtil::toString(_kernel)
           + ", scheduleCount: " + autil::StringUtil::toString(_scheduleCount)
           + ", tryScheduleCount: " + autil::StringUtil::toString(_tryScheduleCount)
           + ", queueLatency: " + autil::StringUtil::toString(queueLatencyUs())
           + ", totalLatencyUs: " + autil::StringUtil::toString(totalLatencyUs());
    ret += "\ntimeline: \n";
    for (const auto &event : _eventList) {
        ret += "enqueue: " +
               autil::StringUtil::toString(event.scheduleInfo.enqueueTime) +
               ", begin: " + autil::StringUtil::toString(event.beginTime) +
               ", end: " + autil::StringUtil::toString(event.endTime);
        ret += "\n";
    }
    return ret;
}

}
