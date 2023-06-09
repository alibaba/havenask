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
    for (const auto &pair : _kernelMetricMap) {
        for (const auto &metric : pair.second) {
            delete metric;
        }
    }
}

KernelMetric *GraphMetric::getKernelMetric(const std::string &bizName,
                                           GraphId graphId,
                                           const std::string &node,
                                           const std::string &kernel)
{
    auto metric = new KernelMetric(graphId, node, kernel);
    autil::ScopedLock lock(_metricLock);
    auto &metricVec = _kernelMetricMap[bizName];
    metricVec.push_back(metric);
    return metric;
}

void GraphMetric::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_kernelMetricMap.size());
    for (const auto &pair : _kernelMetricMap) {
        dataBuffer.write(pair.first);
        dataBuffer.write(pair.second.size());
        for (auto metric : pair.second) {
            dataBuffer.write(*metric);
        }
    }
}

void GraphMetric::deserialize(autil::DataBuffer &dataBuffer) {
    size_t bizCount = 0;
    dataBuffer.read(bizCount);
    for (size_t i = 0; i < bizCount; i++) {
        std::string bizName;
        size_t metricCount = 0;
        dataBuffer.read(bizName);
        dataBuffer.read(metricCount);
        auto &metricVec = _kernelMetricMap[bizName];
        for (size_t i = 0; i < metricCount; i++) {
            auto metric = new KernelMetric();
            try {
                dataBuffer.read(*metric);
            } catch (const autil::legacy::ExceptionBase &e) {
                delete metric;
                return;
            }
            metricVec.push_back(metric);
        }
    }
}

void GraphMetric::merge(GraphMetric &other) {
    autil::ScopedLock lock(_metricLock);
    for (const auto &pair : other._kernelMetricMap) {
        auto it = _kernelMetricMap.find(pair.first);
        if (_kernelMetricMap.end() == it) {
            _kernelMetricMap.insert(pair);
        } else {
            it->second.insert(it->second.end(), pair.second.begin(), pair.second.end());
        }
    }
    other._kernelMetricMap.clear();
}

GraphMetricTime GraphMetric::end() {
    _endTime = autil::TimeUtility::currentTime();
    
    GraphMetricTime time;
    for (const auto &pair : _kernelMetricMap) {
        const auto &metrics = pair.second;
        for (auto metric : metrics) {
            time.queueUs += metric->queueLatencyUs();
            time.computeUs += metric->totalLatencyUs();
        }
    }
    return time;
}

int64_t GraphMetric::getLatency() const {
    return _endTime - _beginTime;
}

void GraphMetric::setNaviSymbolTable(
    const NaviSymbolTablePtr &naviSymbolTable)
{
    _naviSymbolTable = naviSymbolTable;
}

void GraphMetric::fillProto(GraphMetricDef *metricDef) const {
    int64_t computeLatencyUs = 0;
    int64_t queueLatencyUs = 0;
    std::unordered_map<uint64_t, uint32_t> addrMap;
    autil::ScopedLock lock(_metricLock);
    for (const auto &pair : _kernelMetricMap) {
        for (auto metric : pair.second) {
            computeLatencyUs += metric->totalLatencyUs();
            queueLatencyUs += metric->queueLatencyUs();
            metric->fillProto(metricDef->add_metrics(), addrMap);
        }
    }
    if (_naviSymbolTable) {
        _naviSymbolTable->resolveSymbol(addrMap,
                                        metricDef->mutable_perf_symbol_table());
    }
    metricDef->set_begin_time(_beginTime);
    metricDef->set_end_time(_endTime);
    metricDef->set_begin_time_mono_ns(_beginTimeMonoNs);
    metricDef->set_compute_latency_us(computeLatencyUs);
    metricDef->set_queue_latency_us(queueLatencyUs);
}

void GraphMetric::show() const {
    int64_t computeLatencyUs = 0;
    int64_t queueLatencyUs = 0;
    std::cout << "graph metric:" << std::endl;
    for (const auto &pair : _kernelMetricMap) {
        std::cout << "biz: " << pair.first << std::endl;
        for (auto metric : pair.second) {
            std::cout << metric->toString() << std::endl;
            computeLatencyUs += metric->totalLatencyUs();
            queueLatencyUs += metric->queueLatencyUs();
        }
    }
    std::cout << "latency: " << getLatency() / 1000.0 << std::endl;
    std::cout << "total time: " << computeLatencyUs / 1000.0 << std::endl;
    std::cout << "total queue time: " << queueLatencyUs / 1000.0 << std::endl;
    std::cout << "graph metric end" << std::endl;
}

}

