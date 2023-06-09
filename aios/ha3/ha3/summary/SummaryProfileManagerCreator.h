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

#include <stddef.h>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/HitSummarySchema.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/summary/SummaryProfileManager.h"

namespace suez {
namespace turing {
class CavaPluginManager;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace config {
class SummaryProfileConfig;
}  // namespace config
}  // namespace isearch
namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor


namespace isearch {
namespace summary {

class SummaryProfileManagerCreator
{
public:
    SummaryProfileManagerCreator(const config::ResourceReaderPtr &resourceReaderPtr,
                                 const config::HitSummarySchemaPtr &hitSummarySchema,
                                 const suez::turing::CavaPluginManager* cavaPluginManager = nullptr,
                                 kmonitor::MetricsReporter *metricsReporter = NULL)
        : _resourceReaderPtr(resourceReaderPtr)
        , _hitSummarySchema(hitSummarySchema)
        , _cavaPluginManager(cavaPluginManager)
        , _metricsReporter(metricsReporter)
    {
    }
    ~SummaryProfileManagerCreator() {}
public:
    SummaryProfileManagerPtr create(const config::SummaryProfileConfig &config);
    SummaryProfileManagerPtr createFromString(const std::string &configStr);
private:
    config::ResourceReaderPtr _resourceReaderPtr;
    config::HitSummarySchemaPtr _hitSummarySchema;
    const suez::turing::CavaPluginManager *_cavaPluginManager;
    kmonitor::MetricsReporter *_metricsReporter;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryProfileManagerCreator> SummaryProfileManagerCreatorPtr;

} // namespace summary
} // namespace isearch
