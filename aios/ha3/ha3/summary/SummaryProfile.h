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
#include "build_service/plugin/PlugInManager.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/config/SummaryProfileInfo.h"
#include "ha3/summary/SummaryExtractorChain.h"

namespace isearch {
namespace config {
class HitSummarySchema;
}  // namespace config
namespace summary {
class SummaryExtractor;
}  // namespace summary
}  // namespace isearch

namespace isearch {
namespace summary {

class SummaryProfile
{
public:
    SummaryProfile(const config::SummaryProfileInfo &summaryProfileInfo,
                   config::HitSummarySchema *hitSummarySchema);
    ~SummaryProfile();
public:
    SummaryExtractorChain* createSummaryExtractorChain() const;
    std::string getProfileName() const;
    const config::FieldSummaryConfigVec& getFieldSummaryConfig() const {
        return _summaryProfileInfo._fieldSummaryConfigVec;
    }
    const config::SummaryProfileInfo& getSummaryProfileInfo() const {
        return _summaryProfileInfo;
    }
    bool addSummaryExtractor(SummaryExtractor *extractor) {
        if (_summaryExtractorChain) {
            _summaryExtractorChain->addSummaryExtractor(extractor);
            return true;
        }
        return false;
    }

private:
    config::SummaryProfileInfo _summaryProfileInfo;
    SummaryExtractorChain *_summaryExtractorChain;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryProfile> SummaryProfilePtr;

} // namespace summary
} // namespace isearch
