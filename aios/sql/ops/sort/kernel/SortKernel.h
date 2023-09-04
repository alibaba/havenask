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
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/sort/SortInitParam.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "table/ComboComparator.h"
#include "table/Table.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace sql {

class SortKernel : public navi::Kernel {
public:
    SortKernel();
    ~SortKernel();

private:
    SortKernel(const SortKernel &);
    SortKernel &operator=(const SortKernel &);

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    void outputResult(navi::KernelComputeContext &runContext);
    bool doLimitCompute(const navi::DataPtr &data);
    bool doCompute(const navi::DataPtr &data);
    void reportMetrics();
    void incComputeTime();
    void incMergeTime(int64_t time);
    void incCompactTime(int64_t time);
    void incTopKTime(int64_t time);
    void incOutputTime(int64_t time);
    void incTotalTime(int64_t time);
    void incTotalInputCount(size_t count);

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    KERNEL_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    SortInitParam _sortInitParam;
    table::TablePtr _table;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    table::ComboComparatorPtr _comparator;
    std::vector<int32_t> _reuseInputs;
    SortInfo _sortInfo;
    int32_t _opId;
};

typedef std::shared_ptr<SortKernel> SortKernelPtr;
} // namespace sql
