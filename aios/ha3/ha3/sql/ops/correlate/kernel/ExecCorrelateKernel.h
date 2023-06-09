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
#include <unordered_map>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "table/Table.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor
namespace table {
class Column;
class ColumnSchema;
} // namespace table

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

class ExecCorrelateKernel : public navi::Kernel {
public:
    ExecCorrelateKernel();
    ~ExecCorrelateKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    bool initOutputTable(const table::TablePtr inputTable, table::TablePtr outputTable);
    table::TablePtr doUnnestTable(const table::TablePtr &inputTable);
    bool calcOutputTableMultiRowOffset(const table::TablePtr &inputTable,
                                       std::vector<uint32_t> *offset2InputTable);

    bool calcOutputTableOneRowOffset(table::Column &unnestColumn,
                                     const table::ColumnSchema &unnestColumnSchema,
                                     std::vector<uint32_t> *offset2InputTable);
    bool copyUnnestFields(const table::TablePtr &inputTable,
                          table::TablePtr outputTable,
                          const std::vector<uint32_t> &offset2InputTable);
    bool copyOtherFields(const table::TablePtr &inputTable,
                         table::TablePtr outputTable,
                         const std::vector<uint32_t> &offset2InputTable);

private:
    void incComputeTimes();
    void incOutputCount(int64_t count);
    void incTotalTime(int64_t time);
    void reportMetrics();

private:
    int32_t _opId;
    bool _hasOffset = false;
    std::vector<int32_t> _reuseInputs;
    // TODO (kong.cui) now do not use _outputFieldsType
    // std::vector<std::string> _outputFieldsType;

    // default treat offset type as int32_t
    std::string _offsetField;
    std::vector<std::string> _copyFields;
    std::unordered_map<std::string, std::string> _unnestFieldMapper;

    CalcInfo _calcInfo;
    SqlSearchInfoCollector *_sqlSearchInfoCollector = nullptr;
    SqlBizResource *_bizResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    kmonitor::MetricsReporter *_opMetricReporter = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ExecCorrelateKernel> ExecCorrelateKernelPtr;
} // namespace sql
} // namespace isearch
