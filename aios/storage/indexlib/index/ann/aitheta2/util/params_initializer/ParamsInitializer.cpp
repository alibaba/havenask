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
using namespace std;
using namespace autil;
namespace indexlibv2::index::ann {

// 双层聚类阈值
static const std::string STRATIFIED_DOC_COUNT_THRESHOLD = "stratified.doc_count_threshold";
// 每个中心点下平均的doc数量
static const std::string PER_CENTRIOD_DOC_COUNT = "stratified.per_centroid_doc_count";
static const std::string LEGACY_PER_CENTRIOD_DOC_COUNT = "proxima.general.cluster.per_centroid_doc_count";
// 双层聚类的中心点数量比例
static const std::string CENTROID_COUNT_SCALING_FACTOR = "stratified.centroid_count_scaling_factor";
static const uint64_t kStratifiedDocCountThreshold = 500000;
static const uint64_t kPerCentriodDocCount = 200;
static const uint64_t kCentroidCountScalingFactor = 10;
static const uint64_t kMaxCentriodCount = 1000000;
static const uint64_t kMinScanCount = 1000;

bool ParamsInitializer::InitAiThetaMeta(const AithetaIndexConfig& indexConfig, AiThetaMeta& meta)
{
    ANN_CHECK(indexConfig.dimension > 0, "invalid dimension[%u]", indexConfig.dimension);

    FeatureType featureType = FeatureType::FT_FP32;
    meta.set_meta(featureType, indexConfig.dimension);

    MajorOrder orderType = MajorOrder::MO_UNDEFINED;
    if (indexConfig.realtimeConfig.enable) {
        orderType = MajorOrder::MO_ROW;
    }
    meta.set_major_order(orderType);

    string distanceType = indexConfig.distanceType;
    ANN_CHECK(distanceType == SQUARED_EUCLIDEAN || distanceType == INNER_PRODUCT ||
                  distanceType == MIPS_SQUARED_EUCLIDEAN,
              "unknown distance type[%s]", distanceType.c_str());
    meta.set_measure(distanceType, 0, AiThetaParams());

    AUTIL_LOG(DEBUG, "init aitheta index meta[%s]", meta.debug_string().c_str());
    return true;
}

bool ParamsInitializer::InitSearchParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    string indexParamsStr = config.searchConfig.indexParams;
    return ParseValue(indexParamsStr, params, false);
}

bool ParamsInitializer::InitBuildParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    string indexParamsStr = config.buildConfig.indexParams;
    return ParseValue(indexParamsStr, params, true);
}

bool ParamsInitializer::InitRealtimeParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    string paramsStr = config.realtimeConfig.indexParams;
    return ParseValue(paramsStr, params, false);
}

bool ParamsInitializer::ParseValue(const std::string& value, AiThetaParams& params, bool isOffline)
{
    ANN_CHECK(AiThetaParams::ParseFromBuffer(value, &params), "parse[%s] failed", value.c_str());
    UpdateAiThetaParamsKey(params);
    if (isOffline) {
        AUTIL_LOG(INFO, "init index params[%s]", params.debug_string().c_str());
    } else {
        AUTIL_LOG(DEBUG, "init index params[%s]", params.debug_string().c_str());
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
    AUTIL_LOG(DEBUG, "update params from [%s] to [%s]", params.debug_string().c_str(),
              updatedParams.debug_string().c_str());
    params = std::move(updatedParams);
}

void ParamsInitializer::UpdateCentriodCount(size_t docCount, const std::string& paramName, AiThetaParams& params)
{
    if (!params.has(paramName) && docCount > 0) {
        std::string centriodCount = CalcCentriodCount(params, docCount);
        params.set(paramName, centriodCount);
    }
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

string ParamsInitializer::CalcCentriodCount(const AiThetaParams& params, uint64_t docCount)
{
    uint64_t perCentriodDocCount = kPerCentriodDocCount;
    if (params.has(PER_CENTRIOD_DOC_COUNT)) {
        params.get(PER_CENTRIOD_DOC_COUNT, &perCentriodDocCount);
    } else if (params.has(LEGACY_PER_CENTRIOD_DOC_COUNT)) {
        params.get(LEGACY_PER_CENTRIOD_DOC_COUNT, &perCentriodDocCount);
    }

    uint64_t centroidCount = (uint64_t)ceil(docCount * 1.0 / perCentriodDocCount);
    centroidCount = std::min(centroidCount, kMaxCentriodCount);

    uint64_t stratifiedDocCountThreshold = kStratifiedDocCountThreshold;
    params.get(STRATIFIED_DOC_COUNT_THRESHOLD, &stratifiedDocCountThreshold);

    if (docCount <= stratifiedDocCountThreshold) {
        return StringUtil::toString(centroidCount);
    }

    uint64_t centroidCountScalingFactor = kCentroidCountScalingFactor;
    params.get(CENTROID_COUNT_SCALING_FACTOR, &centroidCountScalingFactor);
    uint64_t l2CentroidCount = (uint64_t)ceil(sqrt(centroidCount * 1.0 / centroidCountScalingFactor));
    uint64_t l1CentroidCount = centroidCountScalingFactor * l2CentroidCount;
    return StringUtil::toString(l1CentroidCount) + "*" + StringUtil::toString(l2CentroidCount);
}

AUTIL_LOG_SETUP(indexlib.index, ParamsInitializer);
} // namespace indexlibv2::index::ann
