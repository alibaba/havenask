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
#include "sql/ops/union/UnionKernel.h"

#include <cstdint>
#include <engine/NaviConfigContext.h>
#include <iosfwd>
#include <memory>
#include <utility>

#include "matchdoc/VectorDocStorage.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/ops/util/KernelUtil.h"
#include "table/Row.h"
#include "table/Table.h"

using namespace std;
using namespace table;
using namespace matchdoc;

namespace sql {

UnionKernel::UnionKernel() {}

UnionKernel::~UnionKernel() {}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
void UnionKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name(UNION_OP)
        .inputGroup("input0", TableType::TYPE_ID, navi::IT_OPTIONAL)
        .output("output0", TableType::TYPE_ID);
}

bool UnionKernel::config(navi::KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "reuse_inputs", _reuseInputs, _reuseInputs);
    return true;
}
navi::ErrorCode UnionKernel::compute(navi::KernelComputeContext &runContext) {
    navi::IndexType inputGroupIndex = 0;
    navi::GroupDatas datas;
    if (!runContext.getGroupInput(inputGroupIndex, datas)) {
        SQL_LOG(ERROR, "get input group failed");
        return navi::EC_ABORT;
    }
    for (const auto &streamData : datas) {
        if (streamData.data != nullptr) {
            if (!doCompute(streamData.data)) {
                return navi::EC_ABORT;
            }
        }
    }

    TablePtr table;
    std::swap(table, _table);

    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    if (table) {
        TableDataPtr tableData(new TableData(std::move(table)));
        runContext.setOutput(outputIndex, tableData, datas.eof());
    } else {
        // ATTENTION: `table == nullptr && TableData != nullptr` is not allowed
        runContext.setOutput(outputIndex, nullptr, datas.eof());
    }
    return navi::EC_NONE;
}

bool UnionKernel::doCompute(const navi::DataPtr &data) {
    auto inputTable = KernelUtil::getTable(data, _graphMemoryPoolR, !_reuseInputs.empty());
    if (!inputTable) {
        SQL_LOG(ERROR, "invalid input table");
        return false;
    }

    if (_table == nullptr) {
        _table = inputTable;
    } else {
        if (!_table->merge(inputTable)) {
            SQL_LOG(ERROR, "merge input table failed");
            return false;
        }
        _table->mergeDependentPools(inputTable);
    }
    return true;
}

REGISTER_KERNEL(UnionKernel);

} // namespace sql
