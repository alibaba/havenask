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

#include "kmonitor/client/MetricsReporter.h"
#include "navi/engine/Resource.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsReporterR : public navi::RootResource {
public:
    MetricsReporterR();
    MetricsReporterR(const MetricsReporterPtr &metricsReporter);
    ~MetricsReporterR();
    MetricsReporterR(const MetricsReporterR &) = delete;
    MetricsReporterR &operator=(const MetricsReporterR &) = delete;

public:
    const MetricsReporterPtr &getReporter() const;
    MetricsReporterPtr getQueryMetricsReporter(const MetricsTags &tags, const std::string &subPath);

public:
    static const std::string RESOURCE_ID;

private:
    MetricsReporterPtr _reporter;
};
NAVI_TYPEDEF_PTR(MetricsReporterR);

END_KMONITOR_NAMESPACE(kmonitor);
