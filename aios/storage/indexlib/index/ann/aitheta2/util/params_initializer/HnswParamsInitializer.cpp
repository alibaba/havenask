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
#include "indexlib/index/ann/aitheta2/util/params_initializer/HnswParamsInitializer.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
namespace indexlibv2::index::ann {

bool HnswParamsInitializer::InitNormalSearchParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    ANN_CHECK(ParamsInitializer::InitNormalSearchParams(config, params), "init failed");
    ParamsInitializer::UpdateScanRatio(_docCount, PARAM_HNSW_SEARCHER_MAX_SCAN_RATIO, params,
                                       config.searchConfig.scanCount);
    AUTIL_LOG(DEBUG, "init hnsw search params[%s]", params.debug_string().c_str());
    return true;
}

bool HnswParamsInitializer::InitRealtimeBuildParams(const AithetaIndexConfig& config, AiThetaParams& params,
                                                    bool hasMultiIndex)
{
    ANN_CHECK(ParamsInitializer::InitRealtimeBuildParams(config, params, hasMultiIndex), "init failed");

    AiThetaParams builderParams;
    ANN_CHECK(ParseValue(config.buildConfig.indexParams, builderParams), "parse failed");

    size_t efConstruction = kDefaultStreamerEfConstruction;
    builderParams.get(PARAM_HNSW_BUILDER_EFCONSTRUCTION, &efConstruction);
    params.insert(PARAM_OSWG_STREAMER_EFCONSTRUCTION, efConstruction);
    params.insert(PARAM_HNSW_STREAMER_EFCONSTRUCTION, efConstruction);

    bool enableAdSampling = false;
    builderParams.get(PARAM_HNSW_BUILDER_ENABLE_ADSAMPLING, &enableAdSampling);
    params.insert(PARAM_HNSW_STREAMER_ENABLE_ADSAMPLING, enableAdSampling);

    float slackPruningFactor = 1.0;
    builderParams.get(PARAM_HNSW_BUILDER_SLACK_PRUNING_FACTOR, &slackPruningFactor);
    params.insert(PARAM_HNSW_STREAMER_SLACK_PRUNING_FACTOR, slackPruningFactor);

    if (hasMultiIndex) {
        // 多类目场景下, 必须控制单个index的内存分配chunk大小
        size_t segmentSize = std::max(kDefaultStreamerSegmentSize, config.dimension * sizeof(float) * 16ul);
        params.insert(PARAM_OSWG_STREAMER_SEGMENT_SIZE, segmentSize);
        params.insert(PARAM_HNSW_STREAMER_SEGMENT_SIZE, segmentSize);
    }

    return true;
}

bool HnswParamsInitializer::InitRealtimeSearchParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    ANN_CHECK(ParamsInitializer::InitRealtimeSearchParams(config, params), "init failed");

    AiThetaParams searcherParams;
    ANN_CHECK(ParseValue(config.searchConfig.indexParams, searcherParams), "parse failed");

    if (searcherParams.has(PARAM_HNSW_SEARCHER_EF)) {
        int32_t ef = 0;
        searcherParams.get(PARAM_HNSW_SEARCHER_EF, &ef);
        params.insert(PARAM_OSWG_STREAMER_EF, ef);
        params.insert(PARAM_HNSW_STREAMER_EF, ef);
    }

    if (searcherParams.has(PARAM_HNSW_SEARCHER_MAX_SCAN_RATIO)) {
        float ratio = 0;
        searcherParams.get(PARAM_HNSW_SEARCHER_MAX_SCAN_RATIO, &ratio);
        params.insert(PARAM_OSWG_STREAMER_MAX_SCAN_RATIO, ratio);
        params.insert(PARAM_HNSW_STREAMER_MAX_SCAN_RATIO, ratio);
    }
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, HnswParamsInitializer);
} // namespace indexlibv2::index::ann
