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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/plugin/PlugInManager.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/config/SummaryProfileInfo.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"

namespace suez {
namespace turing {
class CavaPluginManager;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace config {
class HitSummarySchema;
}  // namespace config
namespace summary {
class SummaryProfile;
class SummaryExtractor;
}  // namespace summary
}  // namespace isearch
namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

namespace isearch {
namespace summary {

class SummaryProfileManager : public build_service::plugin::PlugInManager
{
public:
    typedef std::map<std::string, SummaryProfile*> SummaryProfiles;
public:
    SummaryProfileManager(config::ResourceReaderPtr &resourceReaderPtr,
                          config::HitSummarySchema *hitSummarySchema);
    ~SummaryProfileManager();
public:
    const SummaryProfile* getSummaryProfile(const std::string &summaryProfileName) const;
    void addSummaryProfile(SummaryProfile *summaryProfile);
    bool init(const suez::turing::CavaPluginManager *cavaPluginManager,
              kmonitor::MetricsReporter *metricsReporter);
    void setRequiredAttributeFields(const std::vector<std::string> &fieldVec);
    const std::vector<std::string>& getRequiredAttributeFields() const {
        return _requiredAttributeFields;
    }
private:
    void clear();
    bool initSummaryProfile(const suez::turing::CavaPluginManager *cavaPluginManager,
                            kmonitor::MetricsReporter *metricsReporter,
                            SummaryProfile *summaryProfile);
    SummaryExtractor* createSummaryExtractor(const config::ExtractorInfo &extractorInfo,
            const suez::turing::CavaPluginManager *cavaPluginManager,
            kmonitor::MetricsReporter *metricsReporter);

private:
    SummaryProfiles _summaryProfiles;
    std::vector<std::string> _requiredAttributeFields;
    config::HitSummarySchema *_hitSummarySchema;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryProfileManager> SummaryProfileManagerPtr;

} // namespace summary
} // namespace isearch
