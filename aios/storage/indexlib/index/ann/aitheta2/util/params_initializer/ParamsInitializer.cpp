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
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"

#include "indexlib/index/ann/aitheta2/util/EmbeddingUtil.h"
using namespace std;
using namespace autil;
namespace indexlibv2::index::ann {

static const uint64_t kMinScanCount = 1000;

bool ParamsInitializer::InitAiThetaMeta(const AithetaIndexConfig& indexConfig, AiThetaMeta& meta)
{
    ANN_CHECK(indexConfig.dimension > 0, "invalid dimension[%u]", indexConfig.dimension);

    string distanceType = indexConfig.distanceType;
    ANN_CHECK(distanceType == SQUARED_EUCLIDEAN || distanceType == INNER_PRODUCT ||
                  distanceType == MIPS_SQUARED_EUCLIDEAN || distanceType == HAMMING,
              "unknown distance type[%s]", distanceType.c_str());
    meta.set_measure(distanceType, 0, AiThetaParams());

    if (distanceType == HAMMING) {
        uint32_t binaryDimension = EmbeddingUtil::GetBinaryDimension(indexConfig.dimension);
        meta.set_meta(FeatureType::FT_BINARY32, binaryDimension);
    } else {
        meta.set_meta(FeatureType::FT_FP32, indexConfig.dimension);
    }

    MajorOrder orderType = MajorOrder::MO_UNDEFINED;
    if (indexConfig.realtimeConfig.enable) {
        orderType = MajorOrder::MO_ROW;
    }
    meta.set_major_order(orderType);

    AUTIL_LOG(DEBUG, "init aitheta meta[%s]", meta.debug_string().c_str());
    return true;
}

bool ParamsInitializer::InitNormalSearchParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    return ParseValue(config.searchConfig.indexParams, params, false);
}

bool ParamsInitializer::InitNormalBuildParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    return ParseValue(config.buildConfig.indexParams, params, true);
}

bool ParamsInitializer::InitRealtimeBuildParams(const AithetaIndexConfig& config, AiThetaParams& params, bool)
{
    return ParseValue(config.realtimeConfig.indexParams, params, false);
}

bool ParamsInitializer::InitRealtimeSearchParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    return ParseValue(config.realtimeConfig.indexParams, params, false);
}

bool ParamsInitializer::ParseValue(const std::string& value, AiThetaParams& params, bool isOffline)
{
    ANN_CHECK(AiThetaParams::ParseFromBuffer(value, &params), "parse[%s] failed", value.c_str());
    UpdateAiThetaParamsKey(params);
    if (isOffline) {
        AUTIL_LOG(INFO, "parse aitheta params[%s]", params.debug_string().c_str());
    } else {
        AUTIL_LOG(DEBUG, "parse aitheta params[%s]", params.debug_string().c_str());
    }
    return true;
}

void ParamsInitializer::UpdateAiThetaParamsKey(AiThetaParams& params)
{
    AiThetaParams updatedParams;
    for (auto& [key, value] : params.hypercube().cubes()) {
        if (key.size() <= _paramKeyPrefix.size() ||
            key.substr(0, _paramKeyPrefix.size()).compare(_paramKeyPrefix) == 0) {
            updatedParams.insert(key, value);
            continue;
        }
        if (value.compatible<AiThetaParams>()) {
            AiThetaParams paramsValue = value.cast<AiThetaParams>();
            UpdateAiThetaParamsKey(paramsValue);
            updatedParams.insert(_paramKeyPrefix + key, paramsValue);
        } else {
            updatedParams.insert(_paramKeyPrefix + key, value);
        }
    }
    AUTIL_LOG(DEBUG, "update aitheta params from[%s] to[%s]", params.debug_string().c_str(),
              updatedParams.debug_string().c_str());
    params = std::move(updatedParams);
}

void ParamsInitializer::UpdateScanRatio(uint64_t docCount, const std::string& paramName, AiThetaParams& params,
                                        uint64_t miniScanCount)
{
    if (docCount == 0) {
        return;
    }
    float scanRatio = params.get_as_float(paramName);
    uint64_t retScanCount = docCount * scanRatio;
    if (retScanCount < miniScanCount) {
        retScanCount = miniScanCount;
    }
    if (retScanCount < kMinScanCount) {
        retScanCount = kMinScanCount;
    }
    float retScanRatio = retScanCount / (float)docCount;
    if (retScanRatio > 1.0f) {
        retScanRatio = 1.0f;
    }
    params.erase(paramName);
    params.set(paramName, retScanRatio);
}

AUTIL_LOG_SETUP(indexlib.index, ParamsInitializer);
} // namespace indexlibv2::index::ann
