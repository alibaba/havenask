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
#include "monitor/CarbonMonitor.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/KMonitorFactory.h"

BEGIN_CARBON_NAMESPACE(monitor);

CARBON_LOG_SETUP(monitor, CarbonMonitor);
using namespace kmonitor;
using namespace std;

#define MONITOR_NAME "libcarbon3"

KMonitorWorker *CarbonMonitor::_worker = NULL;

CarbonMonitor::CarbonMonitor()
{
}

CarbonMonitor::~CarbonMonitor() {
}

bool CarbonMonitor::init(const std::string& sinkAddr_, const std::string& cluster, const std::string& app) {
    std::string sinkAddr = sinkAddr_.empty() ? DEFAULT_KMON_SPEC : sinkAddr_;
    MetricsConfig config;
    config.set_tenant_name("default");
    config.set_service_name(MONITOR_NAME);
    config.set_sink_address(sinkAddr);
    if (!app.empty()) { // empty value may lead to no metric report
        config.AddGlobalTag("app", app);
    }
    if (!cluster.empty()) {
        config.AddGlobalTag("cluster", cluster);
    }
    config.set_inited(true);

    if (_worker == NULL) {
        _worker = new KMonitorWorker();
    }

    if (!_worker->Init(config)) {
        CARBON_LOG(ERROR, "init kmontior worker failed");
        return false;
    }
    // metric type: xxxx://invalid/lhubic/wuhf65/mx2do6
    KMonitor* kMonitor = _worker->GetKMonitor(MONITOR_NAME);
    kMonitor->Register(METRIC_SERVICE_LATENCY, GAUGE, FATAL);
    kMonitor->Register(METRIC_SERVICE_QPS, QPS, FATAL); // TODO:
    kMonitor->Register(METRIC_PROXY_LATENCY, GAUGE, FATAL);
    kMonitor->Register(METRIC_ENCODE_LATENCY, GAUGE, FATAL);
    kMonitor->Register(METRIC_DECODE_LATENCY, GAUGE, FATAL);
    kMonitor->Register(METRIC_DRIVER_CACHE_LATENCY, GAUGE, FATAL);

    CARBON_LOG(INFO, "monitor started [%s, %s] conf: %s", sinkAddr.c_str(), app.c_str(), ToJsonString(config, true).c_str());

    _worker->Start();
    return true;
}

void CarbonMonitor::stop() {
    if (_worker != NULL) {
        _worker->Shutdown();
        delete _worker;
        _worker = NULL;
    }
}

void CarbonMonitor::report(const char* metric, double value) {
    if (_worker == NULL) {
        return;
    }
    KMonitor* kMonitor = _worker->GetKMonitor(MONITOR_NAME);
    if (!kMonitor) return ;
    kMonitor->Report(metric, value);
}

void CarbonMonitor::report(const char* metric, double value, const TagMap& tags) {
    if (_worker == NULL) {
        return;
    }
    KMonitor* kMonitor = _worker->GetKMonitor(MONITOR_NAME);
    if (!kMonitor) return ;
    MetricsTags mtags;
    for (auto kv : tags) {
        mtags.AddTag(kv.first, kv.second);
    }
    kMonitor->Report(metric, &mtags, value);
}

END_CARBON_NAMESPACE(monitor);

