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

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/ops/calc/CalcWrapper.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace isearch {
namespace sql {
class SqlBizResource;
class SqlQueryResource;
class MetaInfoResource;
class SqlSearchInfoCollector;
} // namespace sql
} // namespace isearch

namespace navi {
class GraphMemoryPoolResource;
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace isearch {
namespace sql {

class CalcKernel : public navi::Kernel {
public:
    CalcKernel();
    ~CalcKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    CalcInitParam _initParam;
    CalcWrapperPtr _calcWrapperPtr;
    SqlBizResource *_bizResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CalcKernel> CalcKernelPtr;
} // namespace sql
} // namespace isearch
