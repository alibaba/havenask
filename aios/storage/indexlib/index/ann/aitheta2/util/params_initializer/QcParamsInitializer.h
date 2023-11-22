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
#include "autil/EnvUtil.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"

namespace indexlibv2::index::ann {

class QcParamsInitializer : public ParamsInitializer
{
public:
    QcParamsInitializer(size_t docCount = 0u) : _docCount(docCount) {}
    ~QcParamsInitializer() = default;

public:
    bool InitNormalSearchParams(const AithetaIndexConfig& indexConfig, AiThetaParams& indexParams) override;
    bool InitNormalBuildParams(const AithetaIndexConfig& indexConfig, AiThetaParams& indexParams) override;
    bool InitRealtimeBuildParams(const AithetaIndexConfig& indexConfig, AiThetaParams& params,
                                 bool hasMultiIndex) override;
    bool InitRealtimeSearchParams(const AithetaIndexConfig& indexConfig, AiThetaParams& params) override;

private:
    void UpdateGpuStreamCount(AiThetaParams& indexParams);
    static void UpdateCentriodCount(uint64_t docCount, const std::string& paramName, AiThetaParams& params);
    static std::string CalcCentriodCount(const AiThetaParams& params, uint64_t count);
    void UpdateCentroidScanRatio(bool isRealtimeParams, AiThetaParams& params);

private:
    size_t _docCount;

private:
    static constexpr const char* PARAM_QC_BUILDER_CENTROID_COUNT = "proxima.qc.builder.centroid_count";

    static constexpr const char* PARAM_QC_SEARCHER_SCAN_RATIO = "proxima.qc.searcher.scan_ratio";
    static constexpr const char* PARAM_QC_SEARCHER_BRUTE_FORCE_THRESHOLD = "proxima.qc.searcher.brute_force_threshold";
    static constexpr const char* PARAM_QC_SEARCHER_GPU_STREAM_COUNT = "proxima.qc.searcher.gpu_stream_count";
    static constexpr const char* PARAM_QC_SEARCHER_OPTIMIZER_PARAMS = "proxima.qc.searcher.optimizer_params";

    static constexpr const char* PARAM_QC_STREAMER_SEGMENT_SIZE = "proxima.qc.streamer.segment_size";
    static constexpr const char* PARAM_QC_STREAMER_SCAN_RATIO = "proxima.qc.streamer.scan_ratio";

    static constexpr const char* PARAM_HC_SEARCHER_SCAN_RATIO = "proxima.hc.searcher.scan_ratio";

    static constexpr const char* GPU_QC_STREAM_COUNT_KEY = "GPU_QC_STREAM_COUNT";
    static constexpr const char* STRATIFIED_DOC_COUNT_THRESHOLD = "stratified.doc_count_threshold"; // 双层聚类阈值
    static constexpr const char* PER_CENTRIOD_DOC_COUNT =
        "stratified.per_centroid_doc_count"; // 每个中心点下平均的doc数量
    static constexpr const char* LEGACY_PER_CENTRIOD_DOC_COUNT = "proxima.general.cluster.per_centroid_doc_count";
    static constexpr const char* CENTROID_COUNT_SCALING_FACTOR =
        "stratified.centroid_count_scaling_factor"; // 双层聚类的中心点数量比例
    static constexpr uint64_t kStratifiedDocCountThreshold = 500000;
    static constexpr uint64_t kPerCentriodDocCount = 200;
    static constexpr uint64_t kCentroidCountScalingFactor = 10;
    static constexpr uint64_t kMaxCentriodCount = 1000000;
    static constexpr const int32_t kDefaultGpuStreamCount = 2;
    AUTIL_LOG_DECLARE();
};

typedef QcParamsInitializer GpuQcParamsInitializer;
typedef std::shared_ptr<QcParamsInitializer> QcParamsInitializerPtr;

} // namespace indexlibv2::index::ann
