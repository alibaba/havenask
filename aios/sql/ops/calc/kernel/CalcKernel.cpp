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
#include "sql/ops/calc/CalcKernel.h"

#include <algorithm>
#include <cstdint>
#include <engine/NaviConfigContext.h>
#include <iosfwd>

#include "autil/TimeUtility.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "sql/common/Log.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/util/KernelUtil.h"
#include "suez/turing/expression/common.h"
#include "table/Table.h"

namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace matchdoc;
using namespace navi;
using namespace autil;
using namespace table;
using namespace suez::turing;

namespace sql {

CalcKernel::CalcKernel() {}

CalcKernel::~CalcKernel() {}

void CalcKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.CalcKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID);
}

bool CalcKernel::config(navi::KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "reuse_inputs", _reuseInputs, _reuseInputs);
    return true;
}

navi::ErrorCode CalcKernel::init(navi::KernelInitContext &context) {
    return navi::EC_NONE;
}

navi::ErrorCode CalcKernel::compute(KernelComputeContext &ctx) {
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, isEof);

    TablePtr table;
    if (data) {
        table = KernelUtil::getTable(data, _graphMemoryPoolR, !_reuseInputs.empty());
        if (!table) {
            SQL_LOG(ERROR, "invalid input table");
            return EC_ABORT;
        }
        if (!_calcWrapperR->compute(table, isEof)) {
            SQL_LOG(ERROR, "filter table error.");
            return EC_ABORT;
        }
    }

    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    TableDataPtr tableData(new TableData(table));
    ctx.setOutput(outputIndex, tableData, isEof);
    return EC_NONE;
}

REGISTER_KERNEL(CalcKernel);

} // namespace sql
