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
#include "sql/ops/sink/SinkKernel.h"

#include <cstdint>
#include <engine/NaviConfigContext.h>
#include <iosfwd>
#include <memory>

#include "matchdoc/VectorDocStorage.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "sql/common/Log.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/ops/util/KernelUtil.h"
#include "table/Row.h"
#include "table/TableUtil.h"

namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace table;
using namespace matchdoc;

namespace sql {

SinkKernel::SinkKernel() {}

SinkKernel::~SinkKernel() {}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
void SinkKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.SinkKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID);
}

bool SinkKernel::config(navi::KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "reuse_inputs", _reuseInputs, _reuseInputs);
    return true;
}

navi::ErrorCode SinkKernel::init(navi::KernelInitContext &context) {
    return navi::EC_NONE;
}

navi::ErrorCode SinkKernel::compute(navi::KernelComputeContext &runContext) {
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool eof = false;
    navi::DataPtr data;
    runContext.getInput(inputIndex, data, eof);
    if (data != nullptr) {
        auto inputTable = KernelUtil::getTable(data, _graphMemoryPoolR, !_reuseInputs.empty());
        if (!inputTable) {
            SQL_LOG(ERROR, "invalid input table");
            return navi::EC_ABORT;
        }
        navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
        SQL_LOG(TRACE2, "sink output table: [%s]", TableUtil::toString(inputTable, 10).c_str());
        TableDataPtr tableData(new TableData(inputTable));
        runContext.setOutput(outputIndex, tableData, eof);
        return navi::EC_NONE;
    }
    if (eof) {
        navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
        runContext.setOutput(outputIndex, nullptr, eof);
    }
    return navi::EC_NONE;
}

REGISTER_KERNEL(SinkKernel);

} // namespace sql
