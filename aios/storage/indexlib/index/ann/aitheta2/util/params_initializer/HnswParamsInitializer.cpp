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

const std::string PARAM_HNSW_SEARCHER_MAX_SCAN_RATIO("proxima.hnsw.searcher.max_scan_ratio");
const std::string PARAM_HNSW_SEARCHER_BRUTE_FORCE_THRESHOLD("proxima.hnsw.searcher.brute_force_threshold");

bool HnswParamsInitializer::InitSearchParams(const AithetaIndexConfig& config, AiThetaParams& params)
{
    ANN_CHECK(ParamsInitializer::InitSearchParams(config, params), "init failed");
    ParamsInitializer::UpdateScanRatio(_docCount, PARAM_HNSW_SEARCHER_MAX_SCAN_RATIO, params,
                                       config.searchConfig.scanCount);
    AUTIL_LOG(DEBUG, "init hnsw search params[%s]", params.debug_string().c_str());
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, HnswParamsInitializer);
} // namespace indexlibv2::index::ann
