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
#include "navi/engine/GraphMetric.h"
#include "navi/engine/NaviResult.h"
#include "autil/TimeUtility.h"
#include "navi/perf/NaviSymbolTable.h"
#include "navi/proto/GraphVis.pb.h"
#include "navi/util/CommonUtil.h"

namespace navi {

GraphMetric::GraphMetric() {
    _beginTime = autil::TimeUtility::currentTime();
    _beginTimeMonoNs = CommonUtil::getTimelineTimeNs();
    _endTime = _beginTime;
}

GraphMetric::~GraphMetric() {
}

void GraphMetric::addKernelMetric(const std::string &bizName, const KernelMetricPtr &metric) {
    auto &metricVec = _kernelMetricMap[bizName];
    metricVec.push_back(metric);
}

void GraphMetric::end() {
    if (_endTime != _beginTime) {
        return;
    }
    _endTime = autil::TimeUtility::currentTime();
    GraphMetricTime time;
    for (const auto &pair : _kernelMetricMap) {
        for (const auto &metric : pair.second) {
            time.queueUs += metric->queueLatencyUs();
            time.computeUs += metric->totalLatencyUs();
        }
    }
    _graphMetricTime = time;
}

const GraphMetricTime &GraphMetric::getGraphMetricTime() const {
    return _graphMetricTime;
}

int64_t GraphMetric::getLatency() const {
    return _endTime - _beginTime;
}

void GraphMetric::fillKernelProto(GraphMetricDef *metricDef,
                                  std::unordered_map<uint64_t, uint32_t> &addrMap,
                                  int64_t &computeLatencyUs,
                                  int64_t &queueLatencyUs) const
{
    for (const auto &pair : _kernelMetricMap) {
        for (const auto &metric : pair.second) {
            metric->fillProto(metricDef->add_metrics(), addrMap);
        }
    }
}

void GraphMetric::toProto(GraphMetricDef *metricDef,
                          StringHashTable *stringHashTable) const
{
    int64_t computeLatencyUs = 0;
    int64_t queueLatencyUs = 0;
    std::unordered_map<uint64_t, uint32_t> addrMap;
    fillKernelProto(metricDef, addrMap, computeLatencyUs, queueLatencyUs);
    if (_naviSymbolTable) {
        _naviSymbolTable->resolveSymbol(
            addrMap, metricDef->mutable_perf_symbol_table(), stringHashTable);
    }
    auto graphMetricTime = getGraphMetricTime();
    metricDef->set_begin_time(_beginTime);
    metricDef->set_end_time(_endTime);
    metricDef->set_begin_time_mono_ns(_beginTimeMonoNs);
    metricDef->set_compute_latency_us(graphMetricTime.computeUs);
    metricDef->set_queue_latency_us(graphMetricTime.queueUs);
}

void GraphMetric::fillResult(NaviResult &result) const {
    result.beginTime = _beginTime;
    result.endTime = _endTime;
    result.queueUs = _graphMetricTime.queueUs;
    result.computeUs = _graphMetricTime.computeUs;
}

void GraphMetric::show() const {
    auto graphMetricTime = getGraphMetricTime();
    std::cout << "graph metric:" << std::endl;
    for (const auto &pair : _kernelMetricMap) {
        std::cout << "biz: " << pair.first << std::endl;
        for (const auto &metric : pair.second) {
            std::cout << metric->toString() << std::endl;
        }
    }
    std::cout << "latency: " << getLatency() / 1000.0 << std::endl;
    std::cout << "total time: " << graphMetricTime.computeUs / 1000.0 << std::endl;
    std::cout << "total queue time: " << graphMetricTime.queueUs / 1000.0 << std::endl;
    std::cout << "graph metric end" << std::endl;
}

}
