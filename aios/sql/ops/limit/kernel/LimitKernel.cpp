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
#include "sql/ops/limit/LimitKernel.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <engine/NaviConfigContext.h>
#include <memory>
#include <string>

#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "sql/common/Log.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/ops/util/KernelUtil.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace table;

namespace sql {

LimitKernel::LimitKernel()
    : _limit(0)
    , _offset(0)
    , _outputCount(0) {}

LimitKernel::~LimitKernel() {
    reportMetrics();
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
void LimitKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.LimitKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID);
}

bool LimitKernel::config(navi::KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "limit", _limit);
    NAVI_JSONIZE(ctx, "offset", _offset, _offset);
    NAVI_JSONIZE(ctx, "reuse_inputs", _reuseInputs, _reuseInputs);
    return true;
}

navi::ErrorCode LimitKernel::init(navi::KernelInitContext &context) {
    return navi::EC_NONE;
}

navi::ErrorCode LimitKernel::compute(navi::KernelComputeContext &runContext) {
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
        _table = inputTable;
        if (_offset > 0) {
            size_t offset = std::min(_offset, _table->getRowCount());
            for (size_t i = 0; i < offset; i++) {
                _table->markDeleteRow(i);
            }
            _table->deleteRows();
            _offset -= offset;
        }
        if (_offset <= 0 && _table->getRowCount() > 0) {
            size_t rowCount = _table->getRowCount();
            if (rowCount < _limit) {
                _limit -= rowCount;
                outputResult(runContext, eof);
                return navi::EC_NONE;
            } else {
                for (size_t i = _limit; i < rowCount; i++) {
                    _table->markDeleteRow(i);
                }
                _table->deleteRows();
                _limit = 0;
                outputResult(runContext, true);
                return navi::EC_NONE;
            }
        }
    }
    if (eof) {
        outputResult(runContext, eof);
    }
    return navi::EC_NONE;
}

void LimitKernel::outputResult(navi::KernelComputeContext &runContext, bool eof) {
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    if (_table) {
        TableDataPtr tableData(new TableData(_table));
        SQL_LOG(TRACE2, "limit output table: [%s]", TableUtil::toString(_table, 10).c_str());
        runContext.setOutput(outputIndex, tableData, eof);
        _outputCount += _table->getRowCount();
        _table.reset();
    } else {
        runContext.setOutput(outputIndex, nullptr, eof);
    }
}

void LimitKernel::reportMetrics() {
    if (_queryMetricReporterR) {
        static const string pathName = "sql.user.ops.LimitKernel";
        auto opMetricReporter = _queryMetricReporterR->getReporter()->getSubReporter(pathName, {});
        REPORT_USER_MUTABLE_METRIC(opMetricReporter, "output_count", _outputCount);
    }
}

REGISTER_KERNEL(LimitKernel);

} // namespace sql
