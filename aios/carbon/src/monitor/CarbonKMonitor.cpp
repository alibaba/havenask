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
#include "monitor/CarbonKMonitor.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(monitor);

CARBON_LOG_SETUP(monitor, CarbonKMonitor);

CarbonKMonitor::CarbonKMonitor()
{
}

CarbonKMonitor::~CarbonKMonitor() {
}

bool CarbonKMonitor::init(const string &serviceName,
                         const string &baseNodePath,
                         uint32_t port)
{
    return true;
}

bool CarbonKMonitor::start() {
    return true;
}

void CarbonKMonitor::stop() {
}

void CarbonKMonitor::report(const nodePath_t &nodePath,
                           const metricName_t &metricName,
                           double value)
{
}

END_CARBON_NAMESPACE(monitor);
