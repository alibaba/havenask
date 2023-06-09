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
#include "indexlib/index/kkv/kkv_metrics_recorder.h"

#include "kmonitor_adapter/Monitor.h"
#include "kmonitor_adapter/MonitorFactory.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVMetricsRecorder);

KKVMetricsRecorder::KKVMetricsRecorder(kmonitor_adapter::Monitor* monitor, const std::string& tableName)
{
    assert(monitor);
    kmonitor_adapter::Monitor::KVVec tags;
    tags.push_back({string("table"), tableName});

    beforeSearchBuildingLatency =
        monitor->registerGaugeMetric("kkv.beforeSearchBuildingLatency", kmonitor::FATAL, tags);
    beforeSearchBuiltLatency = monitor->registerGaugeMetric("kkv.beforeSearchBuiltLatency", kmonitor::FATAL, tags);
    searchKKVLatency = monitor->registerGaugeMetric("kkv.searchKKVLatency", kmonitor::FATAL, tags);
    beforeFetchValueLatency = monitor->registerGaugeMetric("kkv.beforeFetchValueLatency", kmonitor::FATAL, tags);
    beforeFetchCacheLatency = monitor->registerGaugeMetric("kkv.beforeFetchCacheLatency", kmonitor::FATAL, tags);
    updateCacheRatio = monitor->registerGaugeMetric("kkv.updateCacheRatio", kmonitor::FATAL, tags);

    seekRtSegmentCount = monitor->registerGaugeMetric("kkv.seekRtSegmentCount", kmonitor::FATAL, tags);
    seekSegmentCount = monitor->registerGaugeMetric("kkv.seekSegmentCount", kmonitor::FATAL, tags);
    seekSKeyCount = monitor->registerGaugeMetric("kkv.seekSKeyCount", kmonitor::FATAL, tags);
    matchedSKeyCount = monitor->registerGaugeMetric("kkv.matchedSKeyCount", kmonitor::FATAL, tags);

    registerRecorder();
}

KKVMetricsRecorder::~KKVMetricsRecorder() { unregisterRecoder(); }
}} // namespace indexlib::index
