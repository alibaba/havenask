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

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/N2OneKernel.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/ops/agg/AggBase.h"
#include "sql/ops/agg/AggFuncDesc.h"
#include "sql/ops/agg/AggFuncFactoryR.h"
#include "sql/ops/agg/Aggregator.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/QueryMetricReporterR.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace sql {

class AggKernel : public navi::N2OneKernel {
public:
    AggKernel();

private:
    AggKernel(const AggKernel &);
    AggKernel &operator=(const AggKernel &);

public:
    std::string inputPort() const override;
    std::string inputType() const override;
    std::string outputPort() const override;
    std::string outputType() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode collect(const navi::DataPtr &data) override;
    navi::ErrorCode finalize(navi::DataPtr &data) override;

private:
    void patchHintInfo(const std::map<std::string, std::map<std::string, std::string>> &hintsMap);
    void reportMetrics();
    void incComputeTime();
    void incOutputCount(uint32_t count);
    void incTotalTime(int64_t time);
    void incTotalInputCount(size_t count);

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_DEPEND_ON(AggFuncFactoryR, _aggFuncFactoryR);
    KERNEL_DEPEND_ON(CalcInitParamR, _calcInitParamR);
    KERNEL_DEPEND_ON(CalcTableR, _calcTableR);
    KERNEL_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    KERNEL_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    std::vector<std::string> _groupKeyVec;
    std::vector<AggFuncDesc> _aggFuncDesc;
    AggHints _aggHints;
    std::string _scope;
    AggBasePtr _aggBase;
    AggInfo _aggInfo;
    int32_t _opId;
    std::vector<int32_t> _reuseInputs;
};

typedef std::shared_ptr<AggKernel> AggKernelPtr;
} // namespace sql
