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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/ops/sort/SortInitParam.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "table/ComboComparator.h"
#include "table/Table.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
class GraphMemoryPoolResource;
} // namespace navi

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace sql {
class SqlQueryResource;
class MetaInfoResource;
class SqlSearchInfoCollector;
} // namespace sql
} // namespace isearch
namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace isearch {
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
    SortInitParam _sortInitParam;
    table::TablePtr _table;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    table::ComboComparatorPtr _comparator;
    std::vector<int32_t> _reuseInputs;
    kmonitor::MetricsReporter *_queryMetricsReporter;
    SortInfo _sortInfo;
    SqlSearchInfoCollector *_sqlSearchInfoCollector;
    int32_t _opId;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SortKernel> SortKernelPtr;
} // namespace sql
} // namespace isearch
