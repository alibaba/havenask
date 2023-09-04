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
#include "suez/turing/expression/plugin/CavaScorerAdapter.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <stdarg.h>
#include <stdio.h>
#include <utility>
#include <wchar.h>

#include "autil/CommonMacros.h"
#include "cava/common/Common.h"
#include "cava/common/ErrorDef.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/cava/common/CavaJitWrapper.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/cava/common/CavaScorerModuleInfo.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/expression/cava/impl/Ha3CavaScorerParam.h"
#include "suez/turing/expression/cava/impl/MatchDoc.h"
#include "suez/turing/expression/cava/impl/ScorerProvider.h"
#include "suez/turing/expression/provider/ScoringProvider.h"
#include "turing_ops_util/variant/Tracer.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

using namespace std;
using namespace cava;
using namespace kmonitor;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, CavaScorerAdapter);

static const string kScorerPluginQps = "cava.scorerPluginQps";
static const string kScorerExceptionQps = "cava.scorerExceptionQps";

CavaScorerAdapter::CavaScorerAdapter(const KeyValueMap &scorerParameters,
                                     const CavaPluginManager *cavaPluginManager,
                                     MetricsReporter *metricsReporter)
    : Scorer("CavaScorerAdapter")
    , _defaultScorerName("plugins.DefaultScorer")
    , _scorerNameKey("scorer_class_name")
    , _cavaPluginManager(cavaPluginManager)
    , _metricsReporter(metricsReporter)
    , _tracer(nullptr)
    , _traceRefer(nullptr)
    , _scorerObj(nullptr)
    , _scorerModuleInfo(nullptr)
    , _beginRequestSuccessFlag(true)
    , _scoreSuccessFlag(true) {
    auto it = scorerParameters.find(CAVA_DEFAULT_SCORER_CLASS_NAME);
    if (it != scorerParameters.end()) {
        _defaultScorerName = it->second;
    }
    it = scorerParameters.find(CAVA_SCORER_NAME_KEY);
    if (it != scorerParameters.end()) {
        _scorerNameKey = it->second;
    }
}

bool CavaScorerAdapter::beginRequest(ScoringProvider *provider) {
    REPORT_USER_MUTABLE_QPS(_metricsReporter, kScorerPluginQps);
    beginRequestPrepareResource(provider);
    if (_beginRequestSuccessFlag) {
        beginRequestPrepareModuleInfo(provider);
    }
    if (_beginRequestSuccessFlag) {
        beginRequestCreateScorer(provider);
    }
    if (_beginRequestSuccessFlag) {
        beginRequestCallInit(provider);
    }
    return true;
}

void CavaScorerAdapter::beginRequestPrepareResource(ScoringProvider *provider) {
    _tracer = provider->getRequestTracer();
    _traceRefer = provider->getTracerRefer();

    SuezCavaAllocator *cavaAllocator = provider->getCavaAllocator();
    _cavaCtx.reset(new CavaCtx(cavaAllocator));
}

void CavaScorerAdapter::beginRequestPrepareModuleInfo(ScoringProvider *provider) {
    std::string scorerName = getConfigValue(provider, _scorerNameKey);
    if (scorerName.empty()) {
        REQUEST_TRACE(WARN, "scorer name [key=%s] not find in kvpair, use default", _scorerNameKey.c_str());
        scorerName = _defaultScorerName;
    }
    REQUEST_TRACE(INFO, "try to find scorer [%s]", scorerName.c_str());

    ScorerProvider *cavaScorerProvider = provider->getCavaScorerProvider();
    auto cavaJitModule = cavaScorerProvider->getCavaJitModule();
    if (cavaJitModule) {
        _cavaModuleInfo = _cavaPluginManager->createScorerModule(cavaJitModule, scorerName);
        _scorerModuleInfo = dynamic_cast<CavaScorerModuleInfo *>(_cavaModuleInfo.get());
        if (_scorerModuleInfo) {
            REQUEST_TRACE(INFO, "use scorer [%s] in query", scorerName.c_str());
            return;
        }
    }
    REQUEST_TRACE(INFO, "no scorer [%s] in query, skip", scorerName.c_str());
    _cavaModuleInfo = _cavaPluginManager->getCavaModuleInfo(scorerName);
    _scorerModuleInfo = dynamic_cast<CavaScorerModuleInfo *>(_cavaModuleInfo.get());
    if (_scorerModuleInfo) {
        REQUEST_TRACE(INFO, "use scorer [%s] in config", scorerName.c_str());
        return;
    }
    REQUEST_TRACE(INFO, "no scorer [%s] in config, skip", scorerName.c_str());
    setWarning("no scorer [%s] found in both query and config", scorerName.c_str());
    _beginRequestSuccessFlag = false;
}

void CavaScorerAdapter::beginRequestCreateScorer(ScoringProvider *provider) {
    _scorerObj = _scorerModuleInfo->createFunc(_cavaCtx.get());
    if (_scorerObj == nullptr) {
        if (_cavaCtx->exception != ::cava::ExceptionCode::EXC_NONE) {
            REQUEST_TRACE(ERROR,
                          "call [%s]'s create exception\n%s",
                          _scorerModuleInfo->className.c_str(),
                          _cavaCtx->getStackInfo().c_str());
            REPORT_USER_MUTABLE_QPS(_metricsReporter, kScorerExceptionQps);
        }
        _cavaCtx->reset();
        setWarning("create scorer: %s failed", _scorerModuleInfo->className.c_str());
        _beginRequestSuccessFlag = false;
    }
}

bool CavaScorerAdapter::callBeginRequestFunc(ScoringProvider *provider) {
    ScorerProvider *cavaScorerProvider = provider->getCavaScorerProvider();
    if (!_scorerModuleInfo->beginRequestTuringFunc) {
        setWarning("[%s:%d, %s], no beginRequest found", __FILE__, __LINE__, __FUNCTION__);
        _beginRequestSuccessFlag = false;
        return false;
    }
    return _scorerModuleInfo->beginRequestTuringFunc(_scorerObj, _cavaCtx.get(), cavaScorerProvider);
}

void CavaScorerAdapter::beginRequestCallInit(ScoringProvider *provider) {
    bool ret = callBeginRequestFunc(provider);
    if (unlikely(_cavaCtx->exception != ::cava::ExceptionCode::EXC_NONE)) {
        setWarning("call [%s]'s beginRequest exception\n%s",
                   _scorerModuleInfo->className.c_str(),
                   _cavaCtx->getStackInfo().c_str());
        REPORT_USER_MUTABLE_QPS(_metricsReporter, kScorerExceptionQps);
        _cavaCtx->reset();
        _beginRequestSuccessFlag = false;
        return;
    }
    if (unlikely(!ret)) {
        setWarning("call [%s]'s beginRequest method failed", _scorerModuleInfo->className.c_str());
        _beginRequestSuccessFlag = false;
        return;
    }
}

score_t CavaScorerAdapter::score(matchdoc::MatchDoc &matchDoc) {
    if (unlikely(!_beginRequestSuccessFlag || !_scoreSuccessFlag)) {
        return 0.0;
    }
    double score = 0;
    score = _scorerModuleInfo->scoreFunc(_scorerObj, _cavaCtx.get(), (ha3::MatchDoc *)&matchDoc);
    if (unlikely(_cavaCtx->exception != ::cava::ExceptionCode::EXC_NONE)) {
        _scoreSuccessFlag = false;
        setWarning("call [%s]'s scoreFunc exception, score will return 0\n%s",
                   _scorerModuleInfo->className.c_str(),
                   _cavaCtx->getStackInfo().c_str());
        REPORT_USER_MUTABLE_QPS(_metricsReporter, kScorerExceptionQps);
        _cavaCtx->reset();
    }
    SUEZ_RANK_TRACE(TRACE3, matchDoc, "call [%s]'s scoreFunc return %lf", _scorerModuleInfo->className.c_str(), score);
    return score;
}

void CavaScorerAdapter::batchScore(ScorerParam &scorerParam) {
    ha3::Ha3CavaScorerParam ha3CavaScorerParam(scorerParam.reference, scorerParam.scoreDocCount, scorerParam.matchDocs);
    if (unlikely(!_beginRequestSuccessFlag || !_scoreSuccessFlag)) {
        for (size_t i = 0; i < scorerParam.scoreDocCount; ++i) {
            scorerParam.reference->set(scorerParam.matchDocs[i], (score_t)0);
        }
    } else {
        _scorerModuleInfo->batchScoreFunc(_scorerObj, _cavaCtx.get(), &ha3CavaScorerParam);
        if (unlikely(_cavaCtx->exception != ::cava::ExceptionCode::EXC_NONE)) {
            _scoreSuccessFlag = false;
            setWarning("call [%s]'s BatchScoreFunc exception, scores will be set to 0\n%s",
                       _scorerModuleInfo ? _scorerModuleInfo->className.c_str() : "",
                       _cavaCtx->getStackInfo().c_str());
            REPORT_USER_MUTABLE_QPS(_metricsReporter, kScorerExceptionQps);
            _cavaCtx->reset();
            for (size_t i = 0; i < scorerParam.scoreDocCount; ++i) {
                scorerParam.reference->set(scorerParam.matchDocs[i], (score_t)0);
            }
        }
    }
    REQUEST_TRACE(TRACE3,
                  "call [%s]'s BatchScoreFunc, process doc count: %d",
                  _scorerModuleInfo ? _scorerModuleInfo->className.c_str() : "",
                  scorerParam.scoreDocCount);
}

std::string CavaScorerAdapter::getConfigValue(ScoringProvider *provider, const std::string &configKey) {
    return provider->getKVPairValue(configKey);
}

void CavaScorerAdapter::endRequest() {}

Scorer *CavaScorerAdapter::clone() { return new CavaScorerAdapter(*this); }

void CavaScorerAdapter::destroy() { delete this; }

void CavaScorerAdapter::setWarning(const char *fmt, ...) {
    const size_t maxMessageLength = 1024;
    char buffer[maxMessageLength];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buffer, maxMessageLength, fmt, ap);
    va_end(ap);
    REQUEST_TRACE(ERROR, "%s", buffer);
    setWarningInfo(buffer);
}

} // namespace turing
} // namespace suez
