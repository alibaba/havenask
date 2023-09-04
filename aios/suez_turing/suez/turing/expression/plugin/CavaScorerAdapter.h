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

#include <string>

#include "autil/Log.h"
#include "cava/runtime/CavaCtx.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/plugin/Scorer.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor
namespace matchdoc {
class MatchDoc;
template <typename T>
class Reference;
} // namespace matchdoc

namespace suez {
namespace turing {
class CavaScorerModuleInfo;
class ScoringProvider;
class Tracer;

class CavaScorerAdapter : public Scorer {
public:
    CavaScorerAdapter(const KeyValueMap &scorerParameters,
                      const CavaPluginManager *cavaPluginManager,
                      kmonitor::MetricsReporter *metricsReporter);
    ~CavaScorerAdapter() {}

public:
    bool beginRequest(ScoringProvider *provider) override;
    score_t score(matchdoc::MatchDoc &matchDoc) override;
    void batchScore(ScorerParam &scorerParam) override;
    void endRequest() override;
    Scorer *clone() override;
    void destroy() override;

private:
    void beginRequestPrepareResource(ScoringProvider *provider);
    void beginRequestPrepareModuleInfo(ScoringProvider *provider);
    void beginRequestCreateScorer(ScoringProvider *provider);
    virtual bool callBeginRequestFunc(ScoringProvider *provider);
    void beginRequestCallInit(ScoringProvider *provider);

protected:
    virtual std::string getConfigValue(ScoringProvider *provider, const std::string &configKey);
    void setWarning(const char *fmt, ...);

protected:
    std::string _defaultScorerName;
    std::string _scorerNameKey;
    const CavaPluginManager *_cavaPluginManager = nullptr;
    kmonitor::MetricsReporter *_metricsReporter = nullptr;
    // session
    Tracer *_tracer = nullptr;
    matchdoc::Reference<suez::turing::Tracer> *_traceRefer = nullptr;
    CavaCtxPtr _cavaCtx;
    void *_scorerObj = nullptr;
    CavaModuleInfoPtr _cavaModuleInfo;
    CavaScorerModuleInfo *_scorerModuleInfo = nullptr;

    bool _beginRequestSuccessFlag;
    bool _scoreSuccessFlag;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(CavaScorerAdapter);

} // namespace turing
} // namespace suez
