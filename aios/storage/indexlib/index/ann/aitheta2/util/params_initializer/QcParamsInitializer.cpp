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
#include "indexlib/index/ann/aitheta2/util/params_initializer/QcParamsInitializer.h"

using namespace std;
using namespace autil;
namespace indexlibv2::index::ann {

bool QcParamsInitializer::InitNormalBuildParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    ANN_CHECK(ParamsInitializer::InitNormalBuildParams(config, params), "init failed");
    UpdateCentriodCount(_docCount, PARAM_QC_BUILDER_CENTROID_COUNT, params);
    AUTIL_LOG(INFO, "init qc build params[%s]", params.debug_string().c_str());
    return true;
}

bool QcParamsInitializer::InitNormalSearchParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    ANN_CHECK(ParamsInitializer::InitNormalSearchParams(config, params), "init failed");
    ParamsInitializer::UpdateScanRatio(_docCount, PARAM_QC_SEARCHER_SCAN_RATIO, params, config.searchConfig.scanCount);
    UpdateGpuStreamCount(params);

    // UpdateCentroidScanRatio(false, params); 处于兼容升级考虑，暂时disable掉
    AUTIL_LOG(DEBUG, "init qc search params[%s]", params.debug_string().c_str());
    return true;
}

bool QcParamsInitializer::InitRealtimeBuildParams(const AithetaIndexConfig& config, AiThetaParams& params,
                                                  bool hasMultiIndex)
{
    ANN_CHECK(ParamsInitializer::InitRealtimeBuildParams(config, params, hasMultiIndex), "init failed");

    if (hasMultiIndex == false) {
        return true;
    }
    // 多类目场景下, 必须控制单个index的内存分配chunk大小
    const size_t kSegmentSize = std::max(16 * 1024ul, config.dimension * sizeof(float) * 16ul);
    params.insert(PARAM_QC_STREAMER_SEGMENT_SIZE, kSegmentSize);
    return true;
}

bool QcParamsInitializer::InitRealtimeSearchParams(const AithetaIndexConfig& indexConfig, AiThetaParams& streamerParams)
{
    ANN_CHECK(ParamsInitializer::InitRealtimeSearchParams(indexConfig, streamerParams), "init failed");

    AiThetaParams searcherParams;
    ANN_CHECK(ParseValue(indexConfig.searchConfig.indexParams, searcherParams), "parse failed");

    //  用户不配置实时参数时，实时的中心点参数复用全量数据的中心点参数
    if (searcherParams.has(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS)) {
        AiThetaParams searcherOptimizerParams;
        searcherParams.get(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS, &searcherOptimizerParams);
        AiThetaParams streamerOptimizerParams;
        streamerParams.get(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS, &streamerOptimizerParams);
        streamerOptimizerParams.merge(searcherOptimizerParams);
        streamerParams.set(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS, streamerOptimizerParams);
    }

    // 用户不配置实时参数时，实时的scan ratio复用全量数据的scan ratio
    if (searcherParams.has(PARAM_QC_SEARCHER_SCAN_RATIO)) {
        float normalScanRatio = 0.0f;
        searcherParams.get(PARAM_QC_SEARCHER_SCAN_RATIO, &normalScanRatio);
        streamerParams.insert(PARAM_QC_STREAMER_SCAN_RATIO, normalScanRatio);
    }

    UpdateCentroidScanRatio(true, streamerParams);
    return true;
}

void QcParamsInitializer::UpdateCentriodCount(size_t docCount, const std::string& paramName, AiThetaParams& params)
{
    if (!params.has(paramName) && docCount > 0) {
        std::string centriodCount = CalcCentriodCount(params, docCount);
        params.set(paramName, centriodCount);
    }
}

void QcParamsInitializer::UpdateGpuStreamCount(AiThetaParams& indexParams)
{
    // currently gpu_stream_count is a static param,  must share same value
    int32_t gpuStreamCount = autil::EnvUtil::getEnv(GPU_QC_STREAM_COUNT_KEY, kDefaultGpuStreamCount);
    indexParams.set(PARAM_QC_SEARCHER_GPU_STREAM_COUNT, gpuStreamCount);

    AiThetaParams optimizerParams;
    indexParams.get(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS, &optimizerParams);
    optimizerParams.set(PARAM_QC_SEARCHER_GPU_STREAM_COUNT, gpuStreamCount);
    indexParams.set(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS, optimizerParams);
}

string QcParamsInitializer::CalcCentriodCount(const AiThetaParams& params, uint64_t docCount)
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

void QcParamsInitializer::UpdateCentroidScanRatio(bool isRealtimeParams, AiThetaParams& params)
{
    // 中心点的scan ratio一般大于doc的scan ratio,效果更优
    AiThetaParams centroidParams;
    params.get(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS, &centroidParams);
    std::string key = isRealtimeParams ? PARAM_QC_STREAMER_SCAN_RATIO : PARAM_QC_SEARCHER_SCAN_RATIO;
    if (!centroidParams.has(PARAM_HC_SEARCHER_SCAN_RATIO) && params.has(key)) {
        float scanRatio = 0.0f;
        params.get(key, &scanRatio);
        centroidParams.set(PARAM_HC_SEARCHER_SCAN_RATIO, 2.0 * scanRatio);
        params.insert(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS, centroidParams);
    }
}

AUTIL_LOG_SETUP(indexlib.index, QcParamsInitializer);
} // namespace indexlibv2::index::ann
