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

#include "navi/engine/Resource.h"
#include "kmonitor/client/MetricsReporter.h"

namespace suez_navi {

class MetricsReporterR : public navi::Resource
{
public:
    MetricsReporterR();
    ~MetricsReporterR();
    MetricsReporterR(const MetricsReporterR &) = delete;
    MetricsReporterR &operator=(const MetricsReporterR &) = delete;
public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
public:
    const kmonitor::MetricsReporterPtr &getReporter() const;
public:
    static const std::string RESOURCE_ID;
private:
    static bool parseKMonitorTags(const std::string &tagsStr,
                                  std::map<std::string, std::string> &tagsMap);
private:
    std::string _prefix;
    std::string _path;
    kmonitor::MetricsTags _tags;
    kmonitor::MetricsReporterPtr _reporter;
};
NAVI_TYPEDEF_PTR(MetricsReporterR);

}

