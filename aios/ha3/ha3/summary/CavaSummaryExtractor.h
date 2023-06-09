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
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/summary/SummaryExtractor.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"

class CavaCtx;

namespace isearch {
namespace common {
class SummaryHit;
}  // namespace common
namespace summary {
class SummaryExtractorProvider;
}  // namespace summary
}  // namespace isearch
namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor
namespace suez {
namespace turing {
class CavaPluginManager;
class CavaSummaryModuleInfo;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace summary {

class CavaSummaryExtractor : public SummaryExtractor
{
public:
    CavaSummaryExtractor(const KeyValueMap &parameters,
                         const suez::turing::CavaPluginManager *cavaPluginManager,
                         kmonitor::MetricsReporter *metricsReporter);
    ~CavaSummaryExtractor();
public:
    bool beginRequest(SummaryExtractorProvider *provider) override;
    void extractSummary(common::SummaryHit &summaryHit) override;
    CavaSummaryExtractor* clone() override {
       return new CavaSummaryExtractor(*this);
    }
    void destory() override { delete this; }

private:
    const suez::turing::CavaPluginManager *_cavaPluginManager;
    kmonitor::MetricsReporter *_metricsReporter;
    std::string _defaultClassName;
    std::string _summaryNameKey;
    common::Tracer *_tracer;
    void *_summaryObj;
    CavaCtx *_cavaCtx;
    suez::turing::CavaModuleInfoPtr _cavaModuleInfo;
    suez::turing::CavaSummaryModuleInfo *_summaryModuleInfo;

private:
    friend class CavaSummaryExtractorTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CavaSummaryExtractor> CavaSummaryExtractorPtr;

} // namespace summary
} // namespace isearch
