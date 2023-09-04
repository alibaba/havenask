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
#ifndef CARBON_CARBONKMONITOR_H
#define CARBON_CARBONKMONITOR_H

#include "carbon/Log.h"
#include "common/common.h"
#include "monitor/CarbonMonitor.h"
BEGIN_CARBON_NAMESPACE(monitor);

class CarbonKMonitor : public CarbonMonitor
{
public:
    CarbonKMonitor();
    bool init(const std::string &serviceName,
              const std::string &baseNodePath,
              uint32_t port);
    bool start();
    void stop();
    void report(const nodePath_t &nodePath,
                const metricName_t &metricName,
                double value);
    ~CarbonKMonitor();
private:
    CarbonKMonitor(const CarbonKMonitor &);
    CarbonKMonitor& operator=(const CarbonKMonitor &);
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonKMonitor);

END_CARBON_NAMESPACE(monitor);

#endif //CARBON_CARBONKMONITOR_H
