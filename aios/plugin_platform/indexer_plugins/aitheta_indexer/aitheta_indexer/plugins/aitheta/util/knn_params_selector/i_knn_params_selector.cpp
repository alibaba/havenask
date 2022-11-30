/**
 * @file   knn_params_selector.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 17:44:37 2019
 *
 * @brief
 *
 *
 */

#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/i_knn_params_selector.h"
#include <aitheta/algorithm/mips_reformer.h>
#include <autil/HashAlgorithm.h>

#include "aitheta_indexer/plugins/aitheta/util/param_util.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, IKnnParamsSelector);
IKnnParamsSelector::IKnnParamsSelector(const KnnStrategies &strategies) : _strategies(strategies) {}

IKnnParamsSelector::~IKnnParamsSelector() {}

bool IKnnParamsSelector::GetDynamicParams(const IE_NAMESPACE(util)::KeyValueMap &parameters, int32_t docSize,
                                               IE_NAMESPACE(util)::KeyValueMap &params) {
    KnnRangeParams rangeParams;
    if (true == GetDynamicRangeParams(parameters, docSize, rangeParams)) {
        params = rangeParams.params;
        return true;
    }
    return false;
}

bool IKnnParamsSelector::GetDynamicRangeParams(const IE_NAMESPACE(util)::KeyValueMap &parameters, int32_t docSize,
                                             KnnRangeParams &rangeParams) {
    int32_t useDynamicParams = 0;
    ParamUtil::ExtractValue(parameters, USE_DYNAMIC_PARAMS, &useDynamicParams);
    if (!useDynamicParams) {
        IE_LOG(DEBUG, "donnot use dynamic params");
        return true;
    }
    string strategyName(DEFAULT_STRATEGY);
    auto iterator = parameters.find(STRATEGY_NAME);
    if (iterator != parameters.end()) {
        strategyName = iterator->second;
    }
    IE_LOG(DEBUG, "doc size [%d], use strategy[%s]", docSize, strategyName.c_str());

    bool retGetRangeParamFromConfig = true;
    KnnRangeParams rangeParamsFromStrategies;
    auto itr = _strategies.strategy2RangeParams.find(strategyName);
    if (_strategies.strategy2RangeParams.end() != itr) {
        const KnnStrategy &strategy = itr->second;
        retGetRangeParamFromConfig = GetDynamicRangeParamsFromStrategy(strategy, docSize, rangeParamsFromStrategies);
    } else {
        IE_LOG(DEBUG, "cannot get strategy[%s] from global config, use default strategy", strategyName.c_str());
        itr = _strategies.strategy2RangeParams.find(DEFAULT_STRATEGY);
        if (_strategies.strategy2RangeParams.end() != itr) {
            const KnnStrategy &strategy = itr->second;
            retGetRangeParamFromConfig =
                GetDynamicRangeParamsFromStrategy(strategy, docSize, rangeParamsFromStrategies);
        } else {
            IE_LOG(DEBUG, "cannot get default strategy from global config");
        }
    }

    // 通过表的配置获取config
    string rangeParamsContent = "";
    iterator = parameters.find(DYNAMIC_STRATEGY);
    if (iterator != parameters.end()) {
        rangeParamsContent = iterator->second;
    }

    KnnStrategy paramStrategy;
    if (!rangeParamsContent.empty()) {
        try {
            FromJsonString(paramStrategy, rangeParamsContent);
        } catch (const autil::legacy::ExceptionBase &e) {
            IE_LOG(WARN, "deserialize failed. error:[%s], content:[%s].", e.what(), rangeParamsContent.c_str());
        }
    }

    // 如果从表的配置中读取到了参数，将默认策略的参数进行Merge
    if (true == GetDynamicRangeParamsFromStrategy(paramStrategy, docSize, rangeParams)) {
        if (retGetRangeParamFromConfig) {
            IE_LOG(DEBUG, "merge global strategy[%s] and table parameters", strategyName.c_str());
            rangeParams.Merge(rangeParamsFromStrategies);
            return true;
        }
    } else {
        if (retGetRangeParamFromConfig) {
            rangeParams = rangeParamsFromStrategies;
            return true;
        }
    }
    return false;
}

bool IKnnParamsSelector::GetDynamicRangeParamsFromStrategy(const KnnStrategy &strategy, int32_t docSize,
                                                         KnnRangeParams &params) {
    for (const auto rangeParams : strategy.rangeParamsList) {
        if (docSize < rangeParams.upperLimit) {
            params = rangeParams;
            return true;
        }
    }
    return false;
}

IE_LOG_SETUP(aitheta_plugin, NoneLinearKnnParamsSelector);
NoneLinearKnnParamsSelector::NoneLinearKnnParamsSelector(const KnnStrategies &strategies)
    : IKnnParamsSelector(strategies) {}
NoneLinearKnnParamsSelector::~NoneLinearKnnParamsSelector() {}

IndexDistance::Methods IKnnParamsSelector::GetMethodType(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                                                  const std::string& key) {
    auto iterator = parameters.find(key);

    if (iterator == parameters.end()) {
        IE_LOG(ERROR, "failed to find value with key[%s]", key.c_str());
        return IndexDistance::kMethodUnknown;
    }

    if (EnableMipsParams(parameters) && iterator->second == INNER_PRODUCT) {
        return IndexDistance::kMethodFloatSquaredEuclidean;
    }
    if (iterator->second == L2) {
        return IndexDistance::kMethodFloatSquaredEuclidean;
    }
    if (iterator->second == INNER_PRODUCT) {
       return IndexDistance::kMethodFloatInnerProduct;
    }

    IE_LOG(ERROR, "unknown metric type [%s]", iterator->second.c_str());
    return IndexDistance::kMethodUnknown;
}

bool NoneLinearKnnParamsSelector::EnableMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters) {
    return (IE_NAMESPACE(util)::GetValueFromKeyValueMap(parameters, MIPS_ENABLE) == "true") &&
           (IE_NAMESPACE(util)::GetValueFromKeyValueMap(parameters, SEARCH_METRIC_TYPE) == INNER_PRODUCT);
}

bool NoneLinearKnnParamsSelector::InitMeta(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                           aitheta::IndexMeta &meta, bool printParam) {
    int dim = -1;
    ParamUtil::ExtractValue(parameters, DIMENSION, &dim);
    if (EnableMipsParams(parameters)) {
        static const aitheta::MipsReformer kDefaultReformer;
        const auto &mvalStr = IE_NAMESPACE(util)::GetValueFromKeyValueMap(parameters, MIPS_PARAM_MVAL);
        if (mvalStr.empty()) {
            dim += kDefaultReformer.m();
        } else {
            uint32_t mVal;
            if (!autil::StringUtil::fromString(mvalStr, mVal)) {
                IE_LOG(ERROR, "failed to transformer [%s] to uint32_t", mvalStr.c_str());
                return false;
            }
            dim += mVal;
        }
    }
    meta.setDimension(dim);
    auto metricType = GetMethodType(parameters, BUILD_METRIC_TYPE);
    if (metricType == IndexDistance::Methods::kMethodUnknown) {
        metricType = GetMethodType(parameters, SEARCH_METRIC_TYPE);
        IE_LOG(WARN, "failed to find build type with key[%s], use [%s] instead", BUILD_METRIC_TYPE.c_str(),
               SEARCH_METRIC_TYPE.c_str())
    }
    if (metricType == IndexDistance::Methods::kMethodUnknown) {
        IE_LOG(ERROR, "failed to find build type with either [%s] or [%s]", SEARCH_METRIC_TYPE.c_str(),
               BUILD_METRIC_TYPE.c_str())
        return false;
    }
    meta.setMethod(metricType);
    meta.setType(IndexHolder::FeatureTypes::kTypeFloat);
    if (printParam) {
        IE_LOG(DEBUG, "index meta init: [%s = %d], [%s = %d]", DIMENSION.data(), dim, BUILD_METRIC_TYPE.data(),
               metricType);
    }
    return true;
}

bool NoneLinearKnnParamsSelector::InitMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                                 MipsParams &params, bool printParam) {
    if (!EnableMipsParams(parameters)) {
        AUTIL_LOG(ERROR, "mips is not allowed");
        return false;
    }

    static const aitheta::MipsReformer kDefaultReformer;
    const auto &mvalStr = IE_NAMESPACE(util)::GetValueFromKeyValueMap(parameters, MIPS_PARAM_MVAL);
    if (mvalStr.empty()) {
        params.mval = kDefaultReformer.m();
    } else {
        if (!autil::StringUtil::fromString(mvalStr, params.mval)) {
            IE_LOG(ERROR, "failed to transformer [%s] to uint32_t", mvalStr.c_str());
            return false;
        }
        if ((params.mval & 0x03) != 0U) {
            IE_LOG(WARN, "mval better be a multiple of 4");
        }
    }
    const auto &uvalStr = IE_NAMESPACE(util)::GetValueFromKeyValueMap(parameters, MIPS_PARAM_UVAL);
    if (uvalStr.empty()) {
        params.uval = kDefaultReformer.u();
    } else {
        if (!autil::StringUtil::fromString(uvalStr, params.uval)) {
            IE_LOG(ERROR, "failed to transformer [%s] to float", uvalStr.c_str());
            return false;
        }
        if (params.uval <= 0 || params.uval > 1) {
            IE_LOG(ERROR, "%s[%f] is illegal , should be in (0, 1]", MIPS_PARAM_UVAL.c_str(), params.uval);
            return false;
        }
    }
    const auto &normStr = IE_NAMESPACE(util)::GetValueFromKeyValueMap(parameters, MIPS_PARAM_NORM);
    if (normStr.empty()) {
        params.norm = kDefaultReformer.norm();
    } else {
        if (!autil::StringUtil::fromString(normStr, params.norm)) {
            IE_LOG(ERROR, "failed to transformer [%s] to float", normStr.c_str());
            return false;
        }
        if (params.norm <= 0) {
            IE_LOG(ERROR, "%s[%f] is illegal , should be larger than 0", MIPS_PARAM_NORM.c_str(), params.norm);
            return false;
        }
    }

    if (printParam) {
        IE_LOG(DEBUG, "mips params: [mips.enble = true], [%s = %u], [%s = %f], [%s = %f]", MIPS_PARAM_MVAL.c_str(),
               params.mval, MIPS_PARAM_UVAL.c_str(), params.uval, MIPS_PARAM_NORM.c_str(), params.norm);
    }
    params.enable = true;
    return true;
}

int64_t IKnnParamsSelector::GetParamsSignature(const aitheta::IndexParams &indexParams) {
    aitheta::IndexParams signatureIndexParams = indexParams;
    signatureIndexParams.erase(proxima::PARAM_HC_COMMON_MAX_DOC_CNT);
    signatureIndexParams.erase(proxima::PARAM_GRAPH_COMMON_MAX_DOC_CNT);

    const string str = signatureIndexParams.debugString();
    return autil::HashAlgorithm::hashString(str.c_str(), str.size(), 0);
}

IE_NAMESPACE_END(aitheta_plugin);
