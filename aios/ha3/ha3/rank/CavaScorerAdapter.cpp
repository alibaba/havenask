#include <ha3/rank/CavaScorerAdapter.h>
#include <autil/StringTokenizer.h>
#include <ha3/cava/ScorerProvider.h>
#include <suez/turing/expression/cava/impl/Ha3CavaScorerParam.h>
#include <ha3/common/CommonDef.h>
#include <cava/common/ErrorDef.h>

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);

using namespace cava;
BEGIN_HA3_NAMESPACE(rank);

CavaScorerAdapter::CavaScorerAdapter(const suez::turing::KeyValueMap &scorerParameters,
                                     const suez::turing::CavaPluginManagerPtr &cavaPluginManager,
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

END_HA3_NAMESPACE(rank);
