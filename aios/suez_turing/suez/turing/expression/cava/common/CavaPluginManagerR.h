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

#include "kmonitor/client/MetricsReporterR.h"
#include "navi/engine/Resource.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/cava/common/CavaJitWrapperR.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/function/FunctionInfo.h"

namespace suez {
namespace turing {

class CavaPluginManagerR : public navi::Resource {
public:
    CavaPluginManagerR();
    ~CavaPluginManagerR();
    CavaPluginManagerR(const CavaPluginManagerR &) = delete;
    CavaPluginManagerR &operator=(const CavaPluginManagerR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    const suez::turing::CavaPluginManagerPtr &getManager() const;

public:
    static const std::string RESOURCE_ID;

private:
    std::vector<suez::turing::FunctionInfo> _functionInfos;
    kmonitor::MetricsReporterR *_metricReporterR;
    CavaJitWrapperR *_cavaJitWrapperR;
    std::vector<std::string> _autoRegFuncPkgs;
    suez::turing::CavaPluginManagerPtr _manager;
};
NAVI_TYPEDEF_PTR(CavaPluginManagerR);

} // namespace turing
} // namespace suez
