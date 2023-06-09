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
#include "ha3/rank/CavaScorerAdapter.h"

#include <assert.h>

#include "cava/common/Common.h"
#include "cava/runtime/CavaCtx.h"
#include "suez/turing/expression/cava/common/CavaScorerModuleInfo.h"
#include "suez/turing/expression/cava/impl/ScorerProvider.h"
#include "suez/turing/expression/plugin/CavaScorerAdapter.h"
#include "suez/turing/expression/provider/ScoringProvider.h"

#include "ha3/cava/ScorerProvider.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/rank/ScoringProvider.h"
#include "ha3/search/RankResource.h"

namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

using namespace isearch::config;
using namespace isearch::common;

using namespace cava;
namespace isearch {
namespace rank {

CavaScorerAdapter::CavaScorerAdapter(const suez::turing::KeyValueMap &scorerParameters,
                                     const suez::turing::CavaPluginManager *cavaPluginManager,
                                     kmonitor::MetricsReporter *metricsReporter)
    : suez::turing::CavaScorerAdapter(scorerParameters, cavaPluginManager, metricsReporter)
{
}

std::string CavaScorerAdapter::getConfigValue(suez::turing::ScoringProvider *provider,
        const std::string &configKey)
{
    auto *ha3ScoringProvider = dynamic_cast<rank::ScoringProvider*>(provider);
    assert(ha3ScoringProvider != nullptr);
    const common::Request *request = ha3ScoringProvider->getRequest();
    common::ConfigClause *configClause = request->getConfigClause();
    return configClause->getKVPairValue(configKey);
}

bool CavaScorerAdapter::callBeginRequestFunc(suez::turing::ScoringProvider *provider) {
    auto *ha3ScoringProvider = dynamic_cast<rank::ScoringProvider*>(provider);
    assert(ha3ScoringProvider != nullptr);
    auto *ha3ScorerProvider =
        dynamic_cast<ha3::ScorerProvider*>(ha3ScoringProvider->getCavaScorerProvider());
    assert(ha3ScorerProvider != nullptr);

    if (_scorerModuleInfo->beginRequestTuringFunc) {
        return _scorerModuleInfo->beginRequestTuringFunc(
                _scorerObj, _cavaCtx.get(), ha3ScorerProvider);
    } else if (_scorerModuleInfo->beginRequestHa3Func) {
        return _scorerModuleInfo->beginRequestHa3Func(
                _scorerObj, _cavaCtx.get(), ha3ScorerProvider);
    } else {
        setWarning("[%s:%d, %s], no beginRequest found",
                   __FILE__, __LINE__, __FUNCTION__);
        _beginRequestSuccessFlag = false;
        return false;
    }
    return true;
}

suez::turing::Scorer* CavaScorerAdapter::clone() {
    return new CavaScorerAdapter(*this);
}

} // namespace rank
} // namespace isearch
