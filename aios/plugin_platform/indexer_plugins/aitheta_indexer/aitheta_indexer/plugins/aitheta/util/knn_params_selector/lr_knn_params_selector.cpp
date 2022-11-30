/**
 * @file   knn_params_selector.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 17:44:37 2019
 *
 * @brief
 *
 *
 */

#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/lr_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/param_util.h"
#include <aitheta/algorithm/mips_reformer.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, LRKnnParamsSelector);
LRKnnParamsSelector::LRKnnParamsSelector(const KnnStrategies &strategies) : IKnnParamsSelector(strategies) {}
LRKnnParamsSelector::~LRKnnParamsSelector() {}

bool LRKnnParamsSelector::EnableMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters) { return false; }

bool LRKnnParamsSelector::InitMeta(const IE_NAMESPACE(util)::KeyValueMap &parameters, aitheta::IndexMeta &meta,
                                   bool printParam) {
    int dim = -1;
    ParamUtil::ExtractValue(parameters, DIMENSION, &dim);
    meta.setDimension(dim);

    // PAY ATTENTION: use search metric type as build metric type when using Linear Method
    auto metricType = GetMethodType(parameters, SEARCH_METRIC_TYPE);
    if (metricType == IndexDistance::Methods::kMethodUnknown) {
        metricType = GetMethodType(parameters, BUILD_METRIC_TYPE);
    }
    if (metricType == IndexDistance::Methods::kMethodUnknown) {
        IE_LOG(ERROR, "failed to find build type with either [%s] or [%s]", SEARCH_METRIC_TYPE.c_str(),
               BUILD_METRIC_TYPE.c_str())
       return false;
    }
    meta.setMethod(metricType);
    meta.setType(IndexHolder::FeatureTypes::kTypeFloat);
    if (printParam) {
        IE_LOG(INFO, "index meta init: [%s = %d], [%s = %d]", DIMENSION.data(), dim, BUILD_METRIC_TYPE.data(),
               metricType);
    }
    return true;
}

bool LRKnnParamsSelector::InitKnnBuilderParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                               aitheta::IndexParams &indexParams, bool printParam) {
    return true;
}

bool LRKnnParamsSelector::InitKnnSearcherParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                                aitheta::IndexParams &indexParams, bool printParam) {
    return true;
}

bool LRKnnParamsSelector::InitMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters, MipsParams &params,
                                         bool printParam) {
    AUTIL_LOG(ERROR, "No Need to MIPS Reform for Linear Algorithm");
    return false;
}

IE_NAMESPACE_END(aitheta_plugin);
