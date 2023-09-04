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
#ifndef CARBON_CARBONMONITORSINGLETON_H
#define CARBON_CARBONMONITORSINGLETON_H

#include "carbon/Log.h"
#include "common/common.h"
#include "monitor/CarbonMonitor.h"

#define REPORT_METRIC_RAW(nodePath, metricName, value)                  \
    do {                                                                \
        carbon::monitor::CarbonMonitor *monitor                         \
            = carbon::monitor::CarbonMonitorSingleton::getInstance();   \
        if (monitor) {                                                  \
            monitor->report(nodePath, metricName, value);               \
        }                                                               \
    } while (0)                                                         \

#define REPORT_METRIC(nodePath, metricName, value)              \
    REPORT_METRIC_RAW(nodePath, carbon::monitor::metricName, value)

BEGIN_CARBON_NAMESPACE(monitor);

class CarbonMonitorSingleton
{
public:
    ~CarbonMonitorSingleton();
    static CarbonMonitor *getInstance();
private:
    CarbonMonitorSingleton();
    CarbonMonitorSingleton(const CarbonMonitorSingleton &);
    CarbonMonitorSingleton& operator=(const CarbonMonitorSingleton &);
private:
    static CarbonMonitor *_monitor;

private:
CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonMonitorSingleton);

END_CARBON_NAMESPACE(monitor);

#endif //CARBON_CARBONMONITORSINGLETON_H
