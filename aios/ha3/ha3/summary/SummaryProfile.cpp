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
#include "ha3/summary/SummaryProfile.h"

#include <assert.h>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/config/SummaryProfileInfo.h"
#include "ha3/summary/SummaryExtractorChain.h"

using namespace std;
using namespace isearch::config;
namespace isearch {
namespace summary {
AUTIL_LOG_SETUP(ha3, SummaryProfile);

SummaryProfile::SummaryProfile(const SummaryProfileInfo &summaryProfileInfo,
                               config::HitSummarySchema *hitSummarySchema)
    : _summaryProfileInfo(summaryProfileInfo)
{
    _summaryProfileInfo.convertConfigMapToVector(*hitSummarySchema);
    _summaryExtractorChain = new SummaryExtractorChain;
}

SummaryProfile::~SummaryProfile() {
    if (_summaryExtractorChain) {
        _summaryExtractorChain->destroy();
        _summaryExtractorChain = NULL;
    }
}

SummaryExtractorChain* SummaryProfile::createSummaryExtractorChain() const
{
    assert(_summaryExtractorChain);
    SummaryExtractorChain* summaryExtractorChain = _summaryExtractorChain->clone();
    return summaryExtractorChain;
}

string SummaryProfile::getProfileName() const {
    return _summaryProfileInfo._summaryProfileName;
}

} // namespace summary
} // namespace isearch
