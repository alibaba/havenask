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
    bool InitSearchParams(const AithetaIndexConfig& indexConfig, AiThetaParams& indexParams) override;
    bool InitBuildParams(const AithetaIndexConfig& indexConfig, AiThetaParams& indexParams) override;

private:
    void UpdateGpuStreamCount(AiThetaParams& indexParams);

private:
    size_t _docCount;
    static constexpr const int32_t DEFAULT_GPU_STREAM_COUNT = 2;
    static constexpr const char* PARAM_QC_BUILDER_CENTROID_COUNT = "proxima.qc.builder.centroid_count";
    static constexpr const char* PARAM_QC_SEARCHER_SCAN_RATIO = "proxima.qc.searcher.scan_ratio";
    static constexpr const char* PARAM_QC_SEARCHER_BRUTE_FORCE_THRESHOLD = "proxima.qc.searcher.brute_force_threshold";
    static constexpr const char* PARAM_QC_SEARCHER_GPU_STREAM_COUNT = "proxima.qc.searcher.gpu_stream_count";
    static constexpr const char* PARAM_QC_SEARCHER_OPTIMIZER_PARAMS = "proxima.qc.searcher.optimizer_params";

    static constexpr const char* GPU_QC_STREAM_COUNT_KEY = "GPU_QC_STREAM_COUNT";
    AUTIL_LOG_DECLARE();
};

typedef QcParamsInitializer GpuQcParamsInitializer;
typedef QcParamsInitializer CustomizedQcParamsInitializer;

typedef std::shared_ptr<QcParamsInitializer> QcParamsInitializerPtr;

} // namespace indexlibv2::index::ann
