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
#include "kmonitor_adapter/MonitorFactory.h"

#include <errno.h>

#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/KMonitorWorker.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/core/MetricsSystem.h"

using namespace std;
using namespace kmonitor;

extern char* program_invocation_short_name;

namespace kmonitor_adapter {

AUTIL_DECLARE_AND_SETUP_LOGGER(kmonitor, Monitor);
autil::RecursiveThreadMutex MonitorFactory::_mutex;

MonitorFactory::MonitorFactory() {}

void MonitorFactory::init()
{
    AUTIL_LOG(INFO, "monitor factory init");
    if (KMonitorFactory::IsStarted()) {
        _valid = true;
        _owner = false;
        return;
    }

    MetricsConfig metricsConfig;
    metricsConfig.set_tenant_name(getenv("KMONITOR_ADAPTER_TENANT_NAME", "default"));
    metricsConfig.set_service_name(getenv("KMONITOR_ADAPTER_SERVICE_NAME", "monitor_adapter"));
    string sinkAddress(getenv("KMONITOR_ADAPTER_SINK_ADDRESS", ""));
    if (sinkAddress.empty()) {
        sinkAddress = getenv("HIPPO_SLAVE_IP", "127.0.0.1");
        sinkAddress += ":4141";
    }
    metricsConfig.set_sink_address(sinkAddress.c_str());

    const char* app = getenv("KMONITOR_ADAPTER_APP", nullptr);
    if (!app) {
        AUTIL_LOG(WARN, "not report monitor for no KMONITOR_ADAPTER_APP");
        return;
    }
    metricsConfig.set_inited(true);

    if (!KMonitorFactory::Init(metricsConfig)) {
        AUTIL_LOG(ERROR, "init kmonitor factory failed with[%s], app[%s]", ToJsonString(metricsConfig, true).c_str(),
                  app);
        return;
    }
    KMonitorFactory::Start();
    _owner = true;
    _valid = true;

    AUTIL_LOG(INFO, "init kmonitor factory with[%s], app[%s]", ToJsonString(metricsConfig, true).c_str(), app);
    return;
}

MonitorFactory::~MonitorFactory() { close(); }

void MonitorFactory::close()
{
    if (!_valid) {
        return;
    }
    _valid = false;
    for (auto it = _monitorMap.begin(); it != _monitorMap.end(); ++it) {
        delete it->second;
        it->second = nullptr;
    }
    if (_owner) {
        KMonitorFactory::Shutdown();
    }
}

Monitor* MonitorFactory::createMonitor(string serviceName)
{
    autil::ScopedLock lock(_mutex);
    if (!_valid) {
        return nullptr;
    }
    auto iter = _monitorMap.find(serviceName);
    if (iter != _monitorMap.end()) {
        return iter->second;
    }

    KMonitor* kmonitor = KMonitorFactory::GetKMonitor(serviceName, true, false);
    const char* app = getenv("KMONITOR_ADAPTER_APP", "UNKNOWN");
    const char* userTag0 = getenv("KMONITOR_ADAPTER_USER_TAG_0", "UNKNOWN");
    const char* userTag1 = getenv("KMONITOR_ADAPTER_USER_TAG_1", "UNKNOWN");
    const char* hostIpTag = getenv("HIPPO_SLAVE_IP", "UNKNOWN");
    kmonitor->AddTag("app", app);
    kmonitor->AddTag("user_tag_0", userTag0);
    kmonitor->AddTag("user_tag_1", userTag1);
    kmonitor->AddTag("hippo_slave_ip", hostIpTag);
    if (program_invocation_short_name != NULL) {
        kmonitor->AddTag("progName", string(program_invocation_short_name));
    }
    Monitor* monitor = new Monitor(serviceName, &_mutex, kmonitor);
    _monitorMap[serviceName] = monitor;
    return monitor;
}

MonitorFactory* MonitorFactory::getInstance()
{
    static MonitorFactory factory;
    autil::ScopedLock lock(_mutex);
    if (!factory._valid) {
        factory.init();
    }
    return &factory;
}

const char* MonitorFactory::getenv(const char* envName, const char* defaultValue)
{
    const char* ret = ::getenv(envName);
    if (!ret) {
        return defaultValue;
    }
    return ret;
}

} // namespace kmonitor_adapter
