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

bool QcParamsInitializer::InitBuildParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    ANN_CHECK(ParamsInitializer::InitBuildParams(config, params), "init failed");
    UpdateCentriodCount(_docCount, PARAM_QC_BUILDER_CENTROID_COUNT, params);
    AUTIL_LOG(INFO, "init qc build params[%s]", params.debug_string().c_str());
    return true;
}

bool QcParamsInitializer::InitSearchParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    ANN_CHECK(ParamsInitializer::InitSearchParams(config, params), "init failed");
    size_t scanCount = config.searchConfig.scanCount;
    scanCount *= config.searchConfig.scanProportion;
    ParamsInitializer::UpdateScanRatio(_docCount, PARAM_QC_SEARCHER_SCAN_RATIO, params, scanCount);
    UpdateGpuStreamCount(params);
    AUTIL_LOG(DEBUG, "init qc build params[%s]", params.debug_string().c_str());
    return true;
}

void QcParamsInitializer::UpdateGpuStreamCount(AiThetaParams& indexParams)
{
    // currently gpu_stream_count is a static param,  must share same value
    int32_t gpuStreamCount = autil::EnvUtil::getEnv(GPU_QC_STREAM_COUNT_KEY, DEFAULT_GPU_STREAM_COUNT);
    indexParams.set(PARAM_QC_SEARCHER_GPU_STREAM_COUNT, gpuStreamCount);

    AiThetaParams optimizerParams;
    indexParams.get(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS, &optimizerParams);
    optimizerParams.set(PARAM_QC_SEARCHER_GPU_STREAM_COUNT, gpuStreamCount);
    indexParams.set(PARAM_QC_SEARCHER_OPTIMIZER_PARAMS, optimizerParams);
}

AUTIL_LOG_SETUP(indexlib.index, QcParamsInitializer);
} // namespace indexlibv2::index::ann
