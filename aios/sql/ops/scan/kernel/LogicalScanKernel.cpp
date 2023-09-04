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
#include "sql/ops/scan/LogicalScanKernel.h"

#include <algorithm>
#include <engine/NaviConfigContext.h>

#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/ops/scan/ScanUtil.h"
#include "sql/ops/util/KernelUtil.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace sql {

LogicalScanKernel::LogicalScanKernel() {}

LogicalScanKernel::~LogicalScanKernel() {}

void LogicalScanKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.LogicalTableScanKernel").output("output0", TableType::TYPE_ID);
}

bool LogicalScanKernel::config(navi::KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "output_fields_internal", _outputFields);
    KernelUtil::stripName(_outputFields);
    NAVI_JSONIZE(ctx, "output_fields_internal_type", _outputFieldsType, _outputFieldsType);
    return true;
}

navi::ErrorCode LogicalScanKernel::init(navi::KernelInitContext &ctx) {
    return navi::EC_NONE;
}

navi::ErrorCode LogicalScanKernel::compute(navi::KernelComputeContext &context) {
    auto table = ScanUtil::createEmptyTable(
        _outputFields, _outputFieldsType, _graphMemoryPoolR->getPool());
    TableDataPtr tableData(new TableData(table));
    context.setOutput(0, tableData, true);
    return navi::EC_NONE;
}

REGISTER_KERNEL(LogicalScanKernel);

} // namespace sql
