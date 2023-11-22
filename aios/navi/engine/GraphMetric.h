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

#include "navi/engine/KernelMetric.h"
#include <unordered_map>

namespace navi {

class NaviResult;
class GraphMetricDef;
class NaviSymbolTable;
typedef std::shared_ptr<NaviSymbolTable> NaviSymbolTablePtr;
class StringHashTable;

struct GraphMetricTime {
    int64_t queueUs = 0;
    int64_t computeUs = 0;
};

class GraphMetric
{
public:
    GraphMetric();
    ~GraphMetric();
private:
    GraphMetric(const GraphMetric &);
public:
    void setNaviSymbolTable(const NaviSymbolTablePtr &naviSymbolTable) {
        _naviSymbolTable = naviSymbolTable;
    }
    const NaviSymbolTablePtr &getNaviSymbolTable() const {
        return _naviSymbolTable;
    }
    void addKernelMetric(const std::string &bizName, const KernelMetricPtr &metric);
    void end();
    const GraphMetricTime &getGraphMetricTime() const;
    int64_t getLatency() const;
    bool needToProto() const {
        return !_kernelMetricMap.empty();
    }
    void toProto(GraphMetricDef *metric, StringHashTable *stringHashTable) const;
    void show() const;
    void fillResult(NaviResult &result) const;
private:
    void fillKernelProto(GraphMetricDef *metricDef,
                         std::unordered_map<uint64_t, uint32_t> &addrMap,
                         int64_t &computeLatencyUs,
                         int64_t &queueLatencyUs) const;
private:
    NaviSymbolTablePtr _naviSymbolTable;
    int64_t _beginTime;
    int64_t _endTime;
    int64_t _beginTimeMonoNs;
    GraphMetricTime _graphMetricTime;
    std::unordered_map<std::string, std::vector<KernelMetricPtr>> _kernelMetricMap;
};

}
