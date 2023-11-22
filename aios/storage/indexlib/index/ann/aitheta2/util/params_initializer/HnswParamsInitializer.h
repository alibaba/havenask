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

#pragma once
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"

namespace indexlibv2::index::ann {

class HnswParamsInitializer : public ParamsInitializer
{
public:
    HnswParamsInitializer(uint32_t docCount = 0) : _docCount(docCount) {}
    ~HnswParamsInitializer() = default;

public:
    bool InitNormalSearchParams(const AithetaIndexConfig& indexConfig, AiThetaParams& indexParams) override;
    bool InitRealtimeBuildParams(const AithetaIndexConfig& indexConfig, AiThetaParams& params,
                                 bool hasMultiIndex) override;
    bool InitRealtimeSearchParams(const AithetaIndexConfig& config, AiThetaParams& params) override;

private:
    uint32_t _docCount;

private:
    static constexpr const char* PARAM_OSWG_STREAMER_SEGMENT_SIZE = "proxima.oswg.streamer.segment_size";
    static constexpr const char* PARAM_OSWG_STREAMER_EFCONSTRUCTION = "proxima.oswg.streamer.efconstruction";
    static constexpr const char* PARAM_OSWG_STREAMER_EF = "proxima.oswg.streamer.ef";
    static constexpr const char* PARAM_OSWG_STREAMER_MAX_SCAN_RATIO = "proxima.oswg.streamer.max_scan_ratio";

    static constexpr const char* PARAM_HNSW_STREAMER_SEGMENT_SIZE = "proxima.hnsw.streamer.chunk_size";
    static constexpr const char* PARAM_HNSW_STREAMER_EFCONSTRUCTION = "proxima.hnsw.streamer.efconstruction";
    static constexpr const char* PARAM_HNSW_STREAMER_ENABLE_ADSAMPLING = "proxima.hnsw.streamer.enable_adsampling";
    static constexpr const char* PARAM_HNSW_STREAMER_SLACK_PRUNING_FACTOR =
        "proxima.hnsw.streamer.slack_pruning_factor";
    static constexpr const char* PARAM_HNSW_STREAMER_MAX_SCAN_RATIO = "proxima.hnsw.streamer.max_scan_ratio";
    static constexpr const char* PARAM_HNSW_STREAMER_EF = "proxima.hnsw.streamer.ef";

    static constexpr const char* PARAM_HNSW_SEARCHER_BRUTE_FORCE_THRESHOLD =
        "proxima.hnsw.searcher.brute_force_threshold";
    static constexpr const char* PARAM_HNSW_SEARCHER_MAX_SCAN_RATIO = "proxima.hnsw.searcher.max_scan_ratio";
    static constexpr const char* PARAM_HNSW_SEARCHER_EF = "proxima.hnsw.searcher.ef";

    static constexpr const char* PARAM_HNSW_BUILDER_EFCONSTRUCTION = "proxima.hnsw.builder.efconstruction";
    static constexpr const char* PARAM_HNSW_BUILDER_ENABLE_ADSAMPLING = "proxima.hnsw.builder.enable_adsampling";
    static constexpr const char* PARAM_HNSW_BUILDER_SLACK_PRUNING_FACTOR = "proxima.hnsw.builder.slack_pruning_factor";

    static constexpr size_t kDefaultStreamerEfConstruction = 64ul;
    static constexpr size_t kDefaultStreamerSegmentSize = 16 * 1024ul;
    AUTIL_LOG_DECLARE();
};

using OswgParamsInitializer = HnswParamsInitializer;
using QGraphParamsInitializer = HnswParamsInitializer;

} // namespace indexlibv2::index::ann
