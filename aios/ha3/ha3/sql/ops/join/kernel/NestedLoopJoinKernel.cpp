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
#include "ha3/sql/ops/join/NestedLoopJoinKernel.h"

#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/join/JoinBase.h"
#include "ha3/sql/ops/join/JoinInfoCollector.h"
#include "ha3/sql/ops/join/JoinKernelBase.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h" // IWYU pragma: keep
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace table;

namespace isearch {
namespace sql {

const size_t NestedLoopJoinKernel::DEFAULT_BUFFER_LIMIT_SIZE = 1024 * 1024;

NestedLoopJoinKernel::NestedLoopJoinKernel()
    : _bufferLimitSize(DEFAULT_BUFFER_LIMIT_SIZE)
    , _fullTableCreated(false)
    , _leftFullTable(true)
    , _leftEof(false)
    , _rightEof(false) {}

NestedLoopJoinKernel::~NestedLoopJoinKernel() {
    reportMetrics();
}

void NestedLoopJoinKernel::def(navi::KernelDefBuilder &builder) const {
    JoinKernelBase::def(builder);
    builder.name("NestedLoopJoinKernel")
        .input("input0", TableType::TYPE_ID)
        .input("input1", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID);
}

bool NestedLoopJoinKernel::config(navi::KernelConfigContext &ctx) {
    if (!JoinKernelBase::config(ctx)) {
        return false;
    }
    if (_conditionJson.empty()) {
        SQL_LOG(ERROR, "nest loop join condition is empty");
        return false;
    }
    ctx.Jsonize("buffer_limit_size", _bufferLimitSize, _bufferLimitSize);
    return true;
}

navi::ErrorCode NestedLoopJoinKernel::compute(navi::KernelComputeContext &runContext) {
    JoinInfoCollector::incComputeTimes(&_joinInfo);
    uint64_t beginTime = TimeUtility::currentTime();
    navi::PortIndex outPort(0, navi::INVALID_INDEX);
    navi::PortIndex portIndex0(0, navi::INVALID_INDEX);
    navi::PortIndex portIndex1(1, navi::INVALID_INDEX);
    if (!getInput(runContext, portIndex0, true, _bufferLimitSize, _leftBuffer, _leftEof)) {
        SQL_LOG(ERROR, "get left input failed");
        return navi::EC_ABORT;
    } else if (_leftEof && _leftBuffer == nullptr) {
        runContext.setOutput(outPort, nullptr, true);
        return navi::EC_NONE;
    }
    if (!getInput(runContext, portIndex1, false, _bufferLimitSize, _rightBuffer, _rightEof)) {
        SQL_LOG(ERROR, "get right input failed");
        return navi::EC_ABORT;
    } else if (_rightEof && _rightBuffer == nullptr) {
        runContext.setOutput(outPort, nullptr, true);
        return navi::EC_NONE;
    }

    if (!_fullTableCreated && !waitFullTable()) {
        SQL_LOG(ERROR, "wait full table failed");
        return navi::EC_ABORT;
    }
    table::TablePtr outputTable = nullptr;
    if (_fullTableCreated) {
        if (!doCompute(outputTable)) {
            return navi::EC_ABORT;
        }
    }
    bool eof = false;
    uint64_t endTime = TimeUtility::currentTime();
    JoinInfoCollector::incTotalTime(&_joinInfo, endTime - beginTime);
    auto largeTable = _leftFullTable ? _rightBuffer : _leftBuffer;
    if (_leftEof && _rightEof && largeTable->getRowCount() == 0) {
        eof = true;
        SQL_LOG(DEBUG, "join info: [%s]", _joinInfo.ShortDebugString().c_str());
    }
    if (_sqlSearchInfoCollector) {
        _sqlSearchInfoCollector->overwriteJoinInfo(_joinInfo);
    }
    if (outputTable || eof) {
        outputTable->mergeDependentPools(_leftBuffer);
        outputTable->mergeDependentPools(_rightBuffer);
        TableDataPtr tableData(new TableData(outputTable));
        runContext.setOutput(outPort, tableData, eof);
    }
    return navi::EC_NONE;
}

bool NestedLoopJoinKernel::doCompute(table::TablePtr &outputTable) {
    table::TablePtr largeTable;
    table::TablePtr fullTable;
    const std::vector<size_t> *largeTableIndexs = nullptr;
    const std::vector<size_t> *fullTableIndexs = nullptr;
    if (_leftFullTable) {
        largeTableIndexs = &_tableBIndexes;
        fullTableIndexs = &_tableAIndexes;
        largeTable = _rightBuffer;
        fullTable = _leftBuffer;
    } else {
        largeTableIndexs = &_tableAIndexes;
        fullTableIndexs = &_tableBIndexes;
        largeTable = _leftBuffer;
        fullTable = _rightBuffer;
    }
    size_t beforeJoinedRowCount = largeTable->getRowCount();
    while (true) {
        size_t joinedRowCount = joinTable(largeTable, fullTable);
        size_t leftTableSize = _leftFullTable ? _leftBuffer->getRowCount() : joinedRowCount;
        if (!_joinPtr->generateResultTable(*largeTableIndexs,
                                           *fullTableIndexs,
                                           _leftBuffer,
                                           _rightBuffer,
                                           leftTableSize,
                                           outputTable)) {
            SQL_LOG(ERROR, "generate result table failed");
            return false;
        }
        _tableAIndexes.clear();
        _tableBIndexes.clear();
        if (!outputTable) {
            SQL_LOG(ERROR, "generate join result table failed");
            return false;
        }
        outputTable->deleteRows();

        if (!_leftFullTable || (_rightEof && largeTable->getRowCount() == joinedRowCount)) {
            if (!_joinPtr->finish(_leftBuffer, leftTableSize, outputTable)) {
                SQL_LOG(ERROR, "left buffer finish fill table failed");
                return false;
            }
        }

        if (joinedRowCount > 0) {
            largeTable->clearFrontRows(joinedRowCount);
        } else if (_shouldClearTable) {
            largeTable->clearRows();
        }
        if (largeTable->getRowCount() == 0 || outputTable->getRowCount() >= _batchSize) {
            break;
        }
    }

    SQL_LOG(DEBUG,
            "joined row count [%zu], joined table remaining count [%zu],"
            " left eof [%d], right eof [%d]",
            beforeJoinedRowCount - largeTable->getRowCount(),
            largeTable->getRowCount(),
            _leftEof,
            _rightEof);
    if (_joinInfo.totalhashtime() < 5) {
        SQL_LOG(DEBUG,
                "nest loop join output table: [%s]",
                TableUtil::toString(outputTable, 10).c_str());
    }
    return true;
}

bool NestedLoopJoinKernel::waitFullTable() {
    if (_leftBuffer && _leftBuffer->getRowCount() > _bufferLimitSize && _rightBuffer
        && _rightBuffer->getRowCount() > _bufferLimitSize) {
        SQL_LOG(ERROR, "input buffers exceed limit [%lu]", _bufferLimitSize);
        return false;
    }
    // todo optimize
    if (_leftEof) {
        _leftFullTable = true;
        _fullTableCreated = true;
        SQL_LOG(DEBUG,
                " left buffer size[%zu], right buffer size[%zu]",
                _leftBuffer->getRowCount(),
                _rightBuffer->getRowCount());
    } else if (_rightEof) {
        _leftFullTable = false;
        _fullTableCreated = true;
        SQL_LOG(DEBUG,
                " left buffer size[%zu], right buffer size[%zu]",
                _leftBuffer->getRowCount(),
                _rightBuffer->getRowCount());
    }
    return true;
}

size_t NestedLoopJoinKernel::joinTable(const table::TablePtr &largeTable,
                                       const table::TablePtr &fullTable) {
    uint64_t beginJoin = TimeUtility::currentTime();
    size_t joinedRowCount = 0;
    size_t joinedCount = 0;
    size_t largeRowCount = largeTable->getRowCount();
    size_t fullRowCount = fullTable->getRowCount();
    for (size_t i = 0; i < largeRowCount && joinedCount < _batchSize; i++) {
        for (size_t j = 0; j < fullRowCount; j++) {
            joinRow(i, j);
        }
        joinedCount += fullRowCount;
        joinedRowCount = i + 1;
    }
    uint64_t afterJoin = TimeUtility::currentTime();
    JoinInfoCollector::incJoinTime(&_joinInfo, afterJoin - beginJoin);
    return joinedRowCount;
}

REGISTER_KERNEL(NestedLoopJoinKernel);

} // namespace sql
} // namespace isearch
