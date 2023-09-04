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
#ifndef CARBON_CARBONMONITOR_H
#define CARBON_CARBONMONITOR_H

#include "carbon/Log.h"
#include "common/common.h"
#include "kmonitor/client/KMonitorWorker.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(monitor);

typedef std::map<std::string, std::string> TagMap;

#define METRIC_SERVICE_LATENCY "serviceLatency"
#define METRIC_SERVICE_QPS "serviceQPS"
#define METRIC_PROXY_LATENCY "proxyLatency"
#define METRIC_DECODE_LATENCY "decodeLatency"
#define METRIC_ENCODE_LATENCY "encodeLatency"
#define METRIC_DRIVER_CACHE_LATENCY "driver.cache.latency"

class CarbonMonitor
{
public:
    CarbonMonitor();
    static bool init(const std::string& sinkAddr, const std::string& cluster, const std::string& app);
    static void stop();
    static void report(const char* metric, double value);
    static void report(const char* metric, double value, const TagMap& tags);
    ~CarbonMonitor();
private:
    CARBON_LOG_DECLARE();

    static kmonitor::KMonitorWorker *_worker;
};

CARBON_TYPEDEF_PTR(CarbonMonitor);

END_CARBON_NAMESPACE(monitor);

#define MONITOR_REPORT(m, v) CarbonMonitor::report(m, v)
#define MONITOR_REPORT_TAG(m, v, tags) CarbonMonitor::report(m, v, tags)

#endif //CARBON_CARBONMONITOR_H
