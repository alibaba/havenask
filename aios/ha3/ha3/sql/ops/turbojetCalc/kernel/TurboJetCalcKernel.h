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

#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/turbojetCalc/TurboJetCalc.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "table/Table.h"

namespace navi {
class GraphMemoryPoolResource;
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace isearch::sql {
class SqlBizResource;
class SqlQueryResource;
class MetaInfoResource;
class SqlSearchInfoCollector;
} // namespace isearch::sql

namespace isearch::sql::turbojet {

class TurboJetCalcKernel : public navi::Kernel {
public:
    TurboJetCalcKernel();
    ~TurboJetCalcKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    // resources
    SqlBizResource *_bizResource {nullptr};
    SqlQueryResource *_queryResource {nullptr};
    MetaInfoResource *_metaInfoResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;

private:
    // configs
    CalcInitParam _initParam;
    TurboJetCalcPtr _calc;
    // LogicalGraphPtr _node;
    // ProgramPtr _program;

    kmonitor::MetricsReporter *_opMetricReporter {nullptr};

private:
    AUTIL_LOG_DECLARE();
};

using TurboJetCalcKernelPtr = std::shared_ptr<TurboJetCalcKernel>;

} // namespace isearch::sql::turbojet
