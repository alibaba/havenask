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
#include "sql/ops/join/LookupJoinKernel.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <engine/NaviConfigContext.h>
#include <ext/alloc_traits.h>
#include <memory>
#include <set>
#include <stdint.h>
#include <unordered_map>
#include <utility>

#include "autil/StringUtil.h"
#include "navi/common.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/ops/join/LookupNormalR.h"
#include "sql/ops/join/LookupR.h"
#include "sql/ops/remoteScan/RemoteScanR.h"
#include "sql/ops/scan/KKVScanR.h"
#include "sql/ops/scan/KVScanR.h"
#include "sql/ops/scan/NormalScanR.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/SummaryScanR.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "table/Table.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace table;

namespace sql {

static const size_t LOOKUP_BATCH_SIZE = 512;

LookupJoinKernel::LookupJoinKernel()
    : _inputEof(false)
    , _lookupBatchSize(LOOKUP_BATCH_SIZE)
    , _lookupTurncateThreshold(0) {}

LookupJoinKernel::~LookupJoinKernel() {
    reportMetrics();
}

void LookupJoinKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.LookupJoinKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .selectR(
            {RemoteScanR::RESOURCE_ID,
             NormalScanR::RESOURCE_ID,
             SummaryScanR::RESOURCE_ID,
             KVScanR::RESOURCE_ID,
             KKVScanR::RESOURCE_ID},
            [](navi::KernelConfigContext &ctx) {
                std::string tableType;
                auto newCtx = ctx.enter("build_node");
                NAVI_JSONIZE(newCtx, "table_type", tableType);
                std::map<std::string, std::map<std::string, std::string>> hintsMap;
                NAVI_JSONIZE(newCtx, "hints", hintsMap, hintsMap);
                if (ScanInitParamR::isRemoteScan(hintsMap)) {
                    if (tableType != SCAN_KV_TABLE_TYPE) {
                        SQL_LOG(ERROR, "remote table only support kv table now");
                        return navi::INVALID_SELECT_INDEX;
                    } else {
                        return 0;
                    }
                }
                if (tableType == SCAN_NORMAL_TABLE_TYPE) {
                    return 1;
                } else if (tableType == SCAN_SUMMARY_TABLE_TYPE) {
                    return 2;
                } else if (tableType == SCAN_KV_TABLE_TYPE) {
                    return 3;
                } else if (tableType == SCAN_KKV_TABLE_TYPE) {
                    return 4;
                } else {
                    SQL_LOG(ERROR, "not support table type [%s].", tableType.c_str());
                    return navi::INVALID_SELECT_INDEX;
                }
            },
            true,
            BIND_RESOURCE_TO(_scanBase))
        .selectR(
            {LookupNormalR::RESOURCE_ID, LookupR::RESOURCE_ID},
            [](navi::KernelConfigContext &ctx) {
                std::string tableType;
                auto newCtx = ctx.enter("build_node");
                NAVI_JSONIZE(newCtx, "table_type", tableType);
                if (tableType == SCAN_NORMAL_TABLE_TYPE) {
                    return 0;
                } else {
                    return 1;
                }
            },
            true,
            BIND_RESOURCE_TO(_lookupR))
        .resourceConfigKey(ScanInitParamR::RESOURCE_ID, "build_node")
        .resourceConfigKey(CalcInitParamR::RESOURCE_ID, "build_node")
        .resourceConfigKey(WatermarkR::RESOURCE_ID, "build_node")
        .jsonAttrs(R"json(
{
    "summary_require_pk" : false,
    "kv_require_pk" : false,
    "kkv_require_pk" : false
})json");
}

bool LookupJoinKernel::doConfig(navi::KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "lookup_batch_size", _lookupBatchSize, _lookupBatchSize);
    if (_lookupBatchSize == 0) {
        _lookupBatchSize = LOOKUP_BATCH_SIZE;
    }
    return true;
}

bool LookupJoinKernel::doInit() {
    patchLookupHintInfo(_joinParamR->_joinHintMap);
    SQL_LOG(DEBUG,
            "batch size [%ld] lookup batch size [%ld], lookup threshold [%ld].",
            _batchSize,
            _lookupBatchSize,
            _lookupTurncateThreshold);
    return true;
}

navi::ErrorCode LookupJoinKernel::compute(navi::KernelComputeContext &runContext) {
    _joinInfoR->incComputeTimes();

    autil::ScopedTime2 timer;
    const auto &asyncPipe = _scanBase->getAsyncPipe();
    if (asyncPipe) {
        // consume activation data and drop
        navi::DataPtr data;
        bool eof;
        asyncPipe->getData(data, eof);

        if (_scanBase->isWatermarkEnabled()) {
            NAVI_KERNEL_LOG(TRACE3, "wait watermark success, start lookup");
            _scanBase->startAsyncLookup();
            _scanBase->disableWatermark();
            return navi::EC_NONE;
        }
        if (auto ec = finishLastJoin(runContext); ec != navi::EC_NONE) {
            NAVI_KERNEL_LOG(ERROR, "finish last join failed, ec[%d]", ec);
            return ec;
        }
        if (auto ec = startNewLookup(runContext); ec != navi::EC_NONE) {
            NAVI_KERNEL_LOG(ERROR, "start new lookup failed, ec[%d]", ec);
            return ec;
        }
    } else {
        if (auto ec = startNewLookup(runContext); ec != navi::EC_NONE) {
            NAVI_KERNEL_LOG(ERROR, "start new lookup failed, ec[%d]", ec);
            return ec;
        }
        if (auto ec = finishLastJoin(runContext); ec != navi::EC_NONE) {
            NAVI_KERNEL_LOG(ERROR, "finish last join failed, ec[%d]", ec);
            return ec;
        }
    }
    _joinInfoR->incTotalTime(timer.done_us());
    _sqlSearchInfoCollectorR->getCollector()->overwriteJoinInfo(*_joinInfoR->_joinInfo);
    return navi::EC_NONE;
}

navi::ErrorCode LookupJoinKernel::finishLastJoin(navi::KernelComputeContext &runContext) {
    if (!_batch.table) {
        SQL_LOG(TRACE3, "skip invalid batch");
        return navi::EC_NONE;
    }
    bool truncated;
    if (!scanAndJoin(_streamQuery, _batch, _outputTable, truncated)) {
        SQL_LOG(ERROR, "batch scan and join failed");
        return navi::EC_ABORT;
    }
    assert(_outputTable);
    _outputTable->deleteRows();

    if (!_batch.hasNext()) {
        if (!_lookupR->finishJoinEnd(_batch.table, _batch.table->getRowCount(), _outputTable)) {
            // do post join
            SQL_LOG(ERROR,
                    "post join for full table failed, input[\n%s]",
                    TableUtil::toString(_batch.table, 10).c_str());
            return navi::EC_ABORT;
        }
    }
    SQL_LOG(TRACE2,
            "truncated[%d], pending output table[\n%s]",
            truncated,
            TableUtil::tailToString(_outputTable, 10).c_str());
    bool eof = (!_batch.hasNext() && _inputEof) || truncated;
    if (eof || !_batch.hasNext() || _outputTable->getRowCount() >= _batchSize) {
        auto tableData = std::make_shared<TableData>(std::move(_outputTable));
        navi::PortIndex portIndex(0, navi::INVALID_INDEX);
        runContext.setOutput(portIndex, tableData, eof);
    } else {
        runContext.setIgnoreDeadlock();
    }
    return navi::EC_NONE;
}

navi::ErrorCode LookupJoinKernel::startNewLookup(navi::KernelComputeContext &runContext) {
    if (!_batch.hasNext()) {
        if (_inputEof) {
            NAVI_KERNEL_LOG(DEBUG, "input eof, skip last new lookup");
            return navi::EC_NONE;
        }
        navi::PortIndex portIndex(0, navi::INVALID_INDEX);
        TablePtr inputTable;
        if (!getLookupInput(
                runContext, portIndex, !_lookupR->isLeftTableIndexed(), inputTable, _inputEof)) {
            return navi::EC_ABORT;
        }
        _batch.reset(inputTable);
        if (!inputTable) {
            assert(_inputEof && "only allow nullptr table while eof");
            navi::PortIndex portIndex(0, navi::INVALID_INDEX);
            runContext.setOutput(portIndex, nullptr, true);
            return navi::EC_NONE;
        }
    }
    _batch.next(_lookupBatchSize);
    _streamQuery = _lookupR->genFinalStreamQuery(_batch);
    // _streamQuery might be nullptr when left table is multi join

    if (!_scanBase->updateScanQuery(_streamQuery)) {
        SQL_LOG(WARN, "update scan query failed, query [%s]", _streamQuery->toString().c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool LookupJoinKernel::scanAndJoin(const std::shared_ptr<StreamQuery> &streamQuery,
                                   const LookupJoinBatch &batch,
                                   TablePtr &outputTable,
                                   bool &truncated) {
    truncated = false;
    TablePtr streamOutput;
    for (bool eof = false; !eof;) {
        if (!_scanBase->batchScan(streamOutput, eof)) {
            SQL_LOG(ERROR, "scan batch scan failed, query [%s]", streamQuery->toString().c_str());
            return false;
        }
        if (streamOutput == nullptr) {
            SQL_LOG(WARN, "stream output is empty, query [%s]", streamQuery->toString().c_str());
            return false;
        }
        if (_lookupR->isLeftTableIndexed()) {
            incTotalLeftInputTable(streamOutput->getRowCount());
        } else {
            incTotalRightInputTable(streamOutput->getRowCount());
        }
        SQL_LOG(TRACE3,
                "join with isLeft[%d] left[\n%s], right[\n%s]",
                _lookupR->isLeftTableIndexed(),
                TableUtil::toString(batch.table, batch.offset, std::min(batch.count, 10ul)).c_str(),
                TableUtil::toString(streamOutput, 10).c_str());

        size_t outputTableOffset = outputTable ? outputTable->getRowCount() : 0;
        if (!_lookupR->joinTable(batch, streamOutput, outputTable)) {
            SQL_LOG(ERROR, "join table failed");
            return false;
        }
        if (!_lookupR->finishJoin(streamOutput, streamOutput->getRowCount(), outputTable)) {
            SQL_LOG(ERROR, "query stream table finish fill table failed");
            return false;
        }
        SQL_LOG(TRACE2,
                "end batch, joined output[\n%s]",
                TableUtil::toString(_outputTable, outputTableOffset, 10).c_str());
        if (canTruncate(_lookupR->getJoinedCount(), _lookupTurncateThreshold)) {
            // reach truncate threshold, early eof
            truncated = true;
            return true;
        }
    }
    return true;
}

void LookupJoinKernel::patchLookupHintInfo(const map<string, string> &hintsMap) {
    if (hintsMap.empty()) {
        return;
    }
    auto iter = hintsMap.find("lookupBatchSize");
    if (iter != hintsMap.end()) {
        uint32_t lookupBatchSize = 0;
        StringUtil::fromString(iter->second, lookupBatchSize);
        if (lookupBatchSize > 0) {
            _lookupBatchSize = lookupBatchSize;
        }
    }
    _lookupTurncateThreshold = _truncateThreshold;
    iter = hintsMap.find("lookupTurncateThreshold");
    if (iter != hintsMap.end()) {
        uint32_t lookupTurncateSize = 0;
        StringUtil::fromString(iter->second, lookupTurncateSize);
        if (lookupTurncateSize > 0) {
            _lookupTurncateThreshold = lookupTurncateSize;
        }
    }
}

REGISTER_KERNEL(LookupJoinKernel);

} // namespace sql
