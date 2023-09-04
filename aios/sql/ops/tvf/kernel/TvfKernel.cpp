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
#include "sql/ops/tvf/TvfKernel.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/TimeUtility.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "sql/common/Log.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/tvf/InvocationAttr.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "sql/ops/util/KernelUtil.h"
#include "table/Table.h"

namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace navi;
using namespace suez::turing;
using namespace table;

namespace sql {

TvfKernel::TvfKernel() {}

TvfKernel::~TvfKernel() {}

void TvfKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.TvfKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID);
}

bool TvfKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode TvfKernel::init(navi::KernelInitContext &context) {
    return navi::EC_NONE;
}

navi::ErrorCode TvfKernel::compute(KernelComputeContext &ctx) {
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, isEof);
    const auto &initParam = _tvfWrapperR->getInitParam();
    auto inputTable = KernelUtil::getTable(data, _graphMemoryPoolR, !initParam.reuseInputs.empty());
    TablePtr outputTable;
    if (!_tvfWrapperR->compute(inputTable, isEof, outputTable)) {
        SQL_LOG(ERROR, "tvf [%s] compute failed.", initParam.invocation.tvfName.c_str());
        return EC_ABORT;
    }
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    if (outputTable || isEof) {
        if (outputTable && inputTable) {
            outputTable->mergeDependentPools(inputTable);
        }
        TableDataPtr tableData(new TableData(outputTable));
        ctx.setOutput(outputIndex, tableData, isEof);
    }
    return EC_NONE;
}

REGISTER_KERNEL(TvfKernel);

} // namespace sql
