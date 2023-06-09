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

#include "navi/engine/Resource.h"
#include "suez/turing/expression/cava/common/CavaJitWrapper.h"
#include "suez_navi/resource/MetricsReporterR.h"
#include "suez_navi/resource/CavaConfig.h"

namespace suez_navi {

class CavaJitWrapperR : public navi::Resource
{
public:
    CavaJitWrapperR();
    ~CavaJitWrapperR();
    CavaJitWrapperR(const CavaJitWrapperR &) = delete;
    CavaJitWrapperR &operator=(const CavaJitWrapperR &) = delete;
public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
public:
    const suez::turing::CavaJitWrapperPtr &getCavaJitWrapper() const;
public:
    static const std::string RESOURCE_ID;
private:
    std::string _configPath;
    CavaConfig _cavaConfig;
    suez::turing::CavaJitWrapperPtr _cavaJitWrapper;
    MetricsReporterR *_metricReporterR;
};

NAVI_TYPEDEF_PTR(CavaJitWrapperR);

}
