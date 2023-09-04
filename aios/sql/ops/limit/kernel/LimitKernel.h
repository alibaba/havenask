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
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/resource/QueryMetricReporterR.h"
#include "table/Table.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace sql {

class LimitKernel : public navi::Kernel {
public:
    LimitKernel();
    ~LimitKernel();

private:
    LimitKernel(const LimitKernel &);
    LimitKernel &operator=(const LimitKernel &);

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    void outputResult(navi::KernelComputeContext &runContext, bool eof);
    void reportMetrics();

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    KERNEL_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    size_t _limit;
    size_t _offset;
    std::vector<int32_t> _reuseInputs;
    table::TablePtr _table;
    size_t _outputCount;
};

typedef std::shared_ptr<LimitKernel> LimitKernelPtr;
} // namespace sql
