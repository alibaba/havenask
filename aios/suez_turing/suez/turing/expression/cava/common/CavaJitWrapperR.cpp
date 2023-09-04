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
#include "suez/turing/expression/cava/common/CavaJitWrapperR.h"

#include "cava/jit/CavaJit.h"

namespace suez {
namespace turing {

const std::string CavaJitWrapperR::RESOURCE_ID = "cava_jit_wrapper_r";

CavaJitWrapperR::CavaJitWrapperR() {}

CavaJitWrapperR::~CavaJitWrapperR() {}

void CavaJitWrapperR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ)
        .dependOn(kmonitor::MetricsReporterR::RESOURCE_ID, true, BIND_RESOURCE_TO(_metricReporterR));
}

bool CavaJitWrapperR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "config_path", _configPath, _configPath);
    NAVI_JSONIZE(ctx, "cava_config", _cavaConfig, _cavaConfig);
    return true;
}

navi::ErrorCode CavaJitWrapperR::init(navi::ResourceInitContext &ctx) {
    if (!_cavaConfig._enable) {
        NAVI_KERNEL_LOG(INFO, "skip init cava jit wrapper as enable is false");
        return navi::EC_NONE;
    }
    if (!::cava::CavaJit::globalInit()) {
        NAVI_KERNEL_LOG(ERROR, "cava global init failed");
        return navi::EC_ABORT;
    }
    suez::turing::CavaJitWrapperPtr cavaJitWrapper(
        new suez::turing::CavaJitWrapper(_configPath, _cavaConfig, _metricReporterR->getReporter().get()));
    if (!cavaJitWrapper->init()) {
        NAVI_KERNEL_LOG(ERROR, "init CavaJitWrapper failed");
        return navi::EC_ABORT;
    }
    _cavaJitWrapper = cavaJitWrapper;
    return navi::EC_NONE;
}

const suez::turing::CavaJitWrapperPtr &CavaJitWrapperR::getCavaJitWrapper() const { return _cavaJitWrapper; }

const suez::turing::CavaConfig &CavaJitWrapperR::getCavaConfig() const { return _cavaConfig; }

REGISTER_RESOURCE(CavaJitWrapperR);

} // namespace turing
} // namespace suez
