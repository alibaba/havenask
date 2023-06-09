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
#include "ha3/sql/ops/tableMerge/kernel/TableMergeKernel.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/data/TableData.h"
#include "navi/util/ReadyBitMap.h"
#include "autil/StringUtil.h"
#include "table/TableUtil.h"

using namespace std;
using namespace navi;
using namespace table;
using namespace autil;

namespace isearch {
namespace sql {

static const std::string TABLE_MERGE_KERNEL_NAME = "TableMergeKernel";

std::string TableMergeKernel::getName() const {
    return TABLE_MERGE_KERNEL_NAME;
}

std::string TableMergeKernel::dataType() const {
    return TableType::TYPE_ID;
}

InputTypeDef TableMergeKernel::inputType() const {
    return IT_REQUIRE;
}

ErrorCode TableMergeKernel::doInit(KernelInitContext &ctx) {
    auto ec = DefaultMergeKernel::doInit(ctx);
    if (ec != EC_NONE) {
        return ec;
    }
    _dataReadyMap->setOptional(false);
    return EC_NONE;
}

ErrorCode TableMergeKernel::compute(KernelComputeContext &ctx) {
    NAVI_KERNEL_LOG(TRACE3, "table merge kernel start compute, bitmap [%s]",
                    StringUtil::toString(*_dataReadyMap).c_str());
    auto usedPartCount = getUsedPartCount();
    for (auto i = 0; i < usedPartCount; ++i) {
        StreamData streamData;
        auto index = getUsedPartId(i);
        auto ec = getData(ctx, index, streamData);
        if (EC_NONE != ec) {
            NAVI_KERNEL_LOG(ERROR, "get data failed, index: %d", index);
            return ec;
        }
        if (!streamData.hasValue) {
            continue;
        }
        if (streamData.eof && !_dataReadyMap->isFinish(index)) {
            NAVI_KERNEL_LOG(TRACE2, "set finish for partId [%d]", index);
            _dataReadyMap->setFinish(index, true);
        }
        if (streamData.data && !mergeTableData(index, streamData.data)) {
            NAVI_KERNEL_LOG(ERROR, "merge table data from partId [%d] failed", index);
            return EC_ABORT;
        }
    }

    // all input is ready or eof
    if (!outputTableData(ctx)) {
        NAVI_KERNEL_LOG(ERROR, "output table data failed");
        return EC_UNKNOWN;
    }
    ctx.setIgnoreDeadlock();
    return EC_NONE;
}

bool TableMergeKernel::mergeTableData(NaviPartId index, DataPtr &data) {
    auto table = KernelUtil::getTable(data);
    if (!table) {
        NAVI_KERNEL_LOG(ERROR, "get table failed for [%p]", data.get());
        return false;
    }
    NAVI_KERNEL_LOG(TRACE2, "get table [%p] for partId [%d]", table.get(), index);
    NAVI_KERNEL_LOG(TRACE3, "table data [%s] for partId [%d]",
                    TableUtil::toString(table, 5).c_str(), index);
    _dataReadyMap->setReady(index, true);
    if (!_outputTable) {
        _outputTable = table;
    } else {
        if (!_outputTable->merge(table)) {
            NAVI_KERNEL_LOG(ERROR, "merge table failed");
            return false;
        }
    }
    return true;
}

bool TableMergeKernel::outputTableData(KernelComputeContext &ctx) {
    assert(_dataReadyMap->isOk() && "data ready map must be ok");
    TableDataPtr tableData;
    if (_outputTable) {
        tableData.reset(new TableData(_outputTable));
    }
    auto eof = _dataReadyMap->isFinish();
    NAVI_KERNEL_LOG(DEBUG, "output table [%p] data [%p] eof [%d]",
                    _outputTable.get(), tableData.get(), eof);
    if (!ctx.setOutput(OUTPUT_PORT, tableData, eof)) {
        NAVI_KERNEL_LOG(ERROR, "setOutput failed");
        return false;
    }
    _outputTable.reset();
    // release table pool per batch
    releasePool();
    // clear input ready state
    _dataReadyMap->setReady(false);
    return true;
}

REGISTER_KERNEL(TableMergeKernel);

} //end namespace kernel
} //end namespace isearch

