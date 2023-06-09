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
#ifndef NAVI_GRAPHMETRIC_H
#define NAVI_GRAPHMETRIC_H

#include "navi/engine/KernelMetric.h"
#include <unordered_map>

namespace navi {

class GraphMetricDef;
class NaviSymbolTable;
typedef std::shared_ptr<NaviSymbolTable> NaviSymbolTablePtr;

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
    GraphMetric &operator=(const GraphMetric &);
public:
    KernelMetric *getKernelMetric(const std::string &bizName,
                                  GraphId graphId,
                                  const std::string &node,
                                  const std::string &kernel);
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    const std::vector<KernelMetric *> &getKernelMetricVec() const;
    void merge(GraphMetric &other);
    GraphMetricTime end();
    int64_t getLatency() const;
    void setNaviSymbolTable(const NaviSymbolTablePtr &naviSymbolTable);
    void fillProto(GraphMetricDef *metric) const;
    void show() const;
private:
    int64_t _beginTime;
    int64_t _endTime;
    int64_t _beginTimeMonoNs;
    mutable autil::ThreadMutex _metricLock;
    std::unordered_map<std::string, std::vector<KernelMetric *>> _kernelMetricMap;
    NaviSymbolTablePtr _naviSymbolTable;
};

}

#endif //NAVI_GRAPHMETRIC_H
