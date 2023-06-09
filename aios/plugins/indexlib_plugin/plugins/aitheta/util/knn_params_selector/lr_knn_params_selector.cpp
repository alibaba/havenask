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
/**
 * @file   knn_params_selector.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 17:44:37 2019
 *
 * @brief
 *
 *
 */

#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/lr_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/param_util.h"
#include <aitheta/algorithm/mips_reformer.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, LRKnnParamsSelector);
LRKnnParamsSelector::LRKnnParamsSelector(const KnnStrategies &strategies)
    : IKnnParamsSelector(strategies) {}
LRKnnParamsSelector::~LRKnnParamsSelector() {}

bool LRKnnParamsSelector::EnableMipsParams(const indexlib::util::KeyValueMap &parameters) { return false; }

bool LRKnnParamsSelector::EnableGpuSearch(const SchemaParameter &schemaParameter, size_t docNum)  { return false; }

bool LRKnnParamsSelector::InitMeta(const indexlib::util::KeyValueMap &parameters, aitheta::IndexMeta &meta,
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

bool LRKnnParamsSelector::InitBuilderParams(const indexlib::util::KeyValueMap &parameters,
                                            aitheta::IndexParams &indexParams, bool printParam) {
    return true;
}

bool LRKnnParamsSelector::InitSearcherParams(const indexlib::util::KeyValueMap &parameters,
                                             aitheta::IndexParams &indexParams, bool printParam) {
    return true;
}

bool LRKnnParamsSelector::InitMipsParams(const indexlib::util::KeyValueMap &parameters, MipsParams &params,
                                         bool printParam) {
    AUTIL_LOG(ERROR, "No Need to MIPS Reform for Linear Algorithm");
    return false;
}

}
}
