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

#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/SummaryProfileInfo.h"

namespace isearch {
namespace common {
class SummaryHit;
}  // namespace common
namespace summary {
class SummaryExtractorProvider;
}  // namespace summary
}  // namespace isearch

namespace isearch {
namespace summary {

// for plugin compatibility
typedef config::FieldSummaryConfig FieldSummaryConfig;
typedef config::FieldSummaryConfigVec FieldSummaryConfigVec;
typedef config::FieldSummaryConfigMap FieldSummaryConfigMap;

class SummaryExtractor
{
public:
    SummaryExtractor() {}
    virtual ~SummaryExtractor() {}
public:
    virtual void extractSummary(common::SummaryHit &summaryHit) = 0;

    virtual bool beginRequest(SummaryExtractorProvider *provider) { return true; }
    virtual void endRequest() {}

    virtual SummaryExtractor* clone() = 0;
    virtual void destory() = 0;
};

typedef std::shared_ptr<SummaryExtractor> SummaryExtractorPtr;

} // namespace summary
} // namespace isearch

