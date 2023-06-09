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
#include "suez_navi/resource/CavaJitWrapperR.h"

namespace suez_navi {

const std::string CavaJitWrapperR::RESOURCE_ID = "cava_jit_wrapper_r";

CavaJitWrapperR::CavaJitWrapperR()
    : _metricReporterR(nullptr)
{
}

CavaJitWrapperR::~CavaJitWrapperR() {
}

void CavaJitWrapperR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID)
        .depend(MetricsReporterR::RESOURCE_ID, true,
                BIND_RESOURCE_TO(_metricReporterR));
}

bool CavaJitWrapperR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("config_path", _configPath, _configPath);
    _cavaConfig.Jsonize(ctx);
    return true;
}

navi::ErrorCode CavaJitWrapperR::init(navi::ResourceInitContext &ctx) {
    suez::turing::CavaJitWrapperPtr cavaJitWrapper(
        new suez::turing::CavaJitWrapper(
            _configPath, _cavaConfig,
            _metricReporterR->getReporter().get()));
    if (!cavaJitWrapper->init()) {
        NAVI_KERNEL_LOG(ERROR, "init CavaJitWrapper failed");
        return navi::EC_ABORT;
    }
    _cavaJitWrapper = cavaJitWrapper;
    return navi::EC_NONE;
}

const suez::turing::CavaJitWrapperPtr &CavaJitWrapperR::getCavaJitWrapper() const {
    return _cavaJitWrapper;
}

REGISTER_RESOURCE(CavaJitWrapperR);

}

