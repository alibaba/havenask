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

#include "autil/CommonMacros.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/common/ColumnTerm.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/DocIdsQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/TableQuery.h"
#include "ha3/common/Term.h"
#include "ha3/search/MatchDataCollectMatchedInfo.h"
#include "ha3/search/MatchDataCollectorCenter.h"
#include "ha3/search/MatchDataManager.h"
#include "matchdoc/ValueType.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/log/NaviLogger.h"
#include "sql/common/IndexInfo.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/join/JoinBase.h"
#include "sql/ops/join/JoinInfoCollector.h"
#include "sql/ops/join/JoinKernelBase.h"
#include "sql/ops/remoteScan/RemoteScanR.h"
#include "sql/ops/scan/KKVScanR.h"
#include "sql/ops/scan/KVScanR.h"
#include "sql/ops/scan/NormalScanR.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/SummaryScanR.h"
#include "sql/ops/util/KernelUtil.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "suez/turing/navi/QueryMemPoolR.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/ValueTypeSwitch.h"

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace table;
using namespace isearch::common;

namespace sql {

static const size_t LOOKUP_BATCH_SIZE = 512;

void LookupJoinBatch::reset(const std::shared_ptr<table::Table> &table_) {
    table = table_;
    offset = 0;
    count = 0;
}

bool LookupJoinBatch::hasNext() const {
    if (!table || offset + count >= table->getRowCount()) {
        return false;
    } else {
        return true;
    }
}

void LookupJoinBatch::next(size_t batchNum) {
    offset += count;
    if (table) {
        count = std::min(table->getRowCount() - offset, batchNum);
    } else {
        count = 0;
    }
}

LookupJoinKernel::LookupJoinKernel()
    : _inputEof(false)
    , _enableRowDeduplicate(false)
    , _useMatchedRow(false)
    , _leftTableIndexed(false)
    , _disableMatchRowIndexOptimize(false)
    , _lookupBatchSize(LOOKUP_BATCH_SIZE)
    , _lookupTurncateThreshold(0)
    , _hasJoinedCount(0)
    , _lookupColumns(nullptr)
    , _joinColumns(nullptr)
    , _lookupIndexInfos(nullptr) {
    _matchedInfo = new isearch::search::MatchDataCollectMatchedInfo(
        string(HA3_MATCHDATAFIELD_MATCHEDROWINFO), &_rowMatchedInfo);
}

LookupJoinKernel::~LookupJoinKernel() {
    reportMetrics();
    DELETE_AND_SET_NULL(_matchedInfo);
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
        .resourceConfigKey(ScanInitParamR::RESOURCE_ID, "build_node")
        .resourceConfigKey(CalcInitParamR::RESOURCE_ID, "build_node")
        .jsonAttrs(R"json(
{
    "summary_require_pk" : false,
    "kv_require_pk" : false,
    "kkv_require_pk" : false
})json");
    JoinKernelBase::addDepend(builder);
}

bool LookupJoinKernel::config(navi::KernelConfigContext &ctx) {
    if (!JoinKernelBase::config(ctx)) {
        return false;
    }
    // left table has index
    NAVI_JSONIZE(ctx, "left_is_build", _leftTableIndexed, _leftTableIndexed);
    NAVI_JSONIZE(ctx, "lookup_batch_size", _lookupBatchSize, _lookupBatchSize);
    if (_lookupBatchSize == 0) {
        _lookupBatchSize = LOOKUP_BATCH_SIZE;
    }
    auto subCtx = ctx.enter("build_node");
    NAVI_JSONIZE(ctx, "left_table_meta", _leftTableMeta, _leftTableMeta);
    NAVI_JSONIZE(ctx, "right_table_meta", _rightTableMeta, _rightTableMeta);
    _leftTableMeta.extractIndexInfos(_leftIndexInfos);
    _rightTableMeta.extractIndexInfos(_rightIndexInfos);
    KernelUtil::stripName(_leftIndexInfos);
    KernelUtil::stripName(_rightIndexInfos);
    patchLookupHintInfo(_hashHints);
    return true;
}

navi::ErrorCode LookupJoinKernel::init(navi::KernelInitContext &context) {
    navi::ErrorCode baseEc = JoinKernelBase::init(context);
    if (baseEc != navi::EC_NONE) {
        return baseEc;
    }
    if (_leftTableIndexed && (_joinType == SQL_LEFT_JOIN_TYPE || _joinType == SQL_ANTI_JOIN_TYPE)) {
        SQL_LOG(ERROR, "leftIsBuild is conflict with joinType[%s]!", _joinType.c_str());
        return navi::EC_ABORT;
    }
    _useMatchedRow = (nullptr != dynamic_cast<NormalScanR *>(_scanBase));
    if (_leftTableIndexed) {
        _lookupColumns = &_leftJoinColumns;
        _joinColumns = &_rightJoinColumns;
        _lookupIndexInfos = &_leftIndexInfos;
    } else {
        _lookupColumns = &_rightJoinColumns;
        _joinColumns = &_leftJoinColumns;
        _lookupIndexInfos = &_rightIndexInfos;
    }
    SQL_LOG(DEBUG,
            "batch size [%ld] lookup batch size [%ld], lookup threshold [%ld].",
            _batchSize,
            _lookupBatchSize,
            _lookupTurncateThreshold);
    std::string field;
    if (_disableMatchRowIndexOptimize || _useSubR->getUseSub()
        || getDocIdField(*_joinColumns, *_lookupColumns, field)) {
        _useMatchedRow = false;
    }
    for (uint16_t i = 0; i < _joinColumns->size(); i++) {
        if (_lookupIndexInfos->find((*_lookupColumns)[i]) == _lookupIndexInfos->end()) {
            _useMatchedRow = false;
        }
    }

    if (_useMatchedRow) {
        auto normalScanR = dynamic_cast<NormalScanR *>(_scanBase);
        assert(normalScanR);
        _matchedInfo->setMatchDocAllocator(normalScanR->getMatchDocAllocator().get());
        _matchedInfo->setPool(_queryMemPoolR->getPool().get());
        if (!_matchedInfo->init()) {
            SQL_LOG(ERROR,
                    "init matchDataCollectMatchedInfo failed, table [%s].",
                    _scanInitParamR->tableName.c_str());
            return navi::EC_ABORT;
        }
    }
    return navi::EC_NONE;
}

navi::ErrorCode LookupJoinKernel::compute(navi::KernelComputeContext &runContext) {
    JoinInfoCollector::incComputeTimes(&_joinInfo);

    autil::ScopedTime2 timer;
    const auto &asyncPipe = _scanBase->getAsyncPipe();
    if (asyncPipe) {
        // consume activation data and drop
        navi::DataPtr data;
        bool eof;
        asyncPipe->getData(data, eof);

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
    JoinInfoCollector::incTotalTime(&_joinInfo, timer.done_us());
    _sqlSearchInfoCollectorR->getCollector()->overwriteJoinInfo(_joinInfo);
    return navi::EC_NONE;
}

navi::ErrorCode LookupJoinKernel::finishLastJoin(navi::KernelComputeContext &runContext) {
    if (!_batch.table) {
        SQL_LOG(DEBUG, "skip invalid batch");
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
        if (!_leftTableIndexed
            && !_joinPtr->finish(_batch.table, _batch.table->getRowCount(), _outputTable)) {
            // do post join
            SQL_LOG(ERROR,
                    "post join for full table failed, input[\n%s]",
                    TableUtil::toString(_batch.table, 10).c_str());
            return navi::EC_ABORT;
        }
    }

    SQL_LOG(TRACE3,
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
        if (!getLookupInput(runContext, portIndex, !_leftTableIndexed, inputTable, _inputEof)) {
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

    _streamQuery = genFinalStreamQuery(_batch);
    if (_useMatchedRow) {
        auto normalScanR = dynamic_cast<NormalScanR *>(_scanBase);
        assert(normalScanR);
        auto matchDataManager = normalScanR->getMatchDataManager();
        matchDataManager->reset();
        matchDataManager->getMatchDataCollectorCenter().subscribe(_matchedInfo);
    }
    // _streamQuery might be nullptr when left table is multi join
    if (!_scanBase->updateScanQuery(_streamQuery)) {
        SQL_LOG(WARN, "update scan query failed, query [%s]", _streamQuery->toString().c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool LookupJoinKernel::scanAndJoin(StreamQueryPtr streamQuery,
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
        if (_leftTableIndexed) {
            incTotalLeftInputTable(streamOutput->getRowCount());
        } else {
            incTotalRightInputTable(streamOutput->getRowCount());
        }
        SQL_LOG(TRACE3,
                "join with isLeft[%d] left[\n%s], right[\n%s]",
                _leftTableIndexed,
                TableUtil::toString(batch.table, batch.offset, std::min(batch.count, 10ul)).c_str(),
                TableUtil::toString(streamOutput, 10).c_str());

        size_t outputTableOffset = outputTable ? outputTable->getRowCount() : 0;
        if (!joinTable(batch, streamOutput, outputTable)) {
            SQL_LOG(ERROR, "join table failed");
            return false;
        }
        if (_leftTableIndexed
            && !_joinPtr->finish(streamOutput, streamOutput->getRowCount(), outputTable)) {
            SQL_LOG(ERROR, "query stream table finish fill table failed");
            return false;
        }
        SQL_LOG(TRACE3,
                "end batch, joined output[\n%s]",
                TableUtil::toString(_outputTable, outputTableOffset, 10).c_str());
        if (canTruncate(_hasJoinedCount, _lookupTurncateThreshold)) {
            // reach truncate threshold, early eof
            truncated = true;
            return true;
        }
    }
    return true;
}

void LookupJoinKernel::createMatchedRowIndexs(const TablePtr &streamOutput) {
    SQL_LOG(DEBUG, "use matched info join optimize");
    assert(streamOutput);
    Column *col = streamOutput->getColumn(string(HA3_MATCHDATAFIELD_MATCHEDROWINFO));
    assert(col);
    auto colData = col->getColumnData<MultiUInt64>();
    assert(colData);
    size_t rowCount = streamOutput->getRowCount();
    vector<size_t> indexA, indexB;
    for (size_t i = 0; i < rowCount; ++i) {
        const auto &datas = colData->get(streamOutput->getRow(i));
        size_t nums = datas.size();
        indexA.insert(indexA.end(), nums, i);
        for (size_t idx = 0; idx < nums; ++idx) {
            indexB.push_back(datas[idx]);
        }
    }
    if (_leftTableIndexed) {
        _tableAIndexes.swap(indexA);
        _tableBIndexes.swap(indexB);
    } else {
        _tableAIndexes.swap(indexB);
        _tableBIndexes.swap(indexA);
    }
    _hasJoinedCount += _tableAIndexes.size();
}

bool LookupJoinKernel::joinTable(const LookupJoinBatch &batch,
                                 const TablePtr &streamOutput,
                                 TablePtr &outputTable) {
    if (isDocIdsOptimize()) {
        if (!docIdsJoin(batch, streamOutput)) {
            SQL_LOG(ERROR, "unexpected docIds join failed");
            return false;
        }
    } else if (_useMatchedRow && _scanInitParamR->tableType == SCAN_NORMAL_TABLE_TYPE) {
        createMatchedRowIndexs(streamOutput);
    } else if (!doHashJoin(batch, streamOutput)) {
        SQL_LOG(ERROR, "join table failed");
        return false;
    }

    const vector<size_t> &leftTableIndexs = _tableAIndexes;
    const vector<size_t> &rightTableIndexs = _tableBIndexes;
    const auto &leftTable = _leftTableIndexed ? streamOutput : batch.table;
    const auto &rightTable = _leftTableIndexed ? batch.table : streamOutput;
    if (!_joinPtr->generateResultTable(leftTableIndexs,
                                       rightTableIndexs,
                                       leftTable,
                                       rightTable,
                                       leftTable->getRowCount(),
                                       outputTable)) {
        SQL_LOG(ERROR, "generate result table failed");
        return false;
    }
    _tableAIndexes.clear();
    _tableBIndexes.clear();
    return true;
}

bool LookupJoinKernel::docIdsJoin(const LookupJoinBatch &batch, const TablePtr &streamOutput) {
    SQL_LOG(DEBUG, "use docIds join optimize");
    if (batch.count != streamOutput->getRowCount()) {
        SQL_LOG(ERROR,
                "unexpected input table size [%lu] != joined table size [%lu]",
                batch.count,
                streamOutput->getRowCount());
        return false;
    }
    reserveJoinRow(batch.count);
    for (size_t i = 0; i < batch.count; ++i) {
        joinRow(i + batch.offset, i);
    }
    return true;
}

bool LookupJoinKernel::doHashJoin(const LookupJoinBatch &batch, const TablePtr &streamTable) {
    SQL_LOG(DEBUG, "use hash map join optimize");
    size_t rowCount = streamTable->getRowCount();
    if (rowCount == 0) {
        return true;
    }
    // todo: once per scan eof
    // create hashmap form left table, then calculate right table's hash values
    if (!createHashMap(streamTable, 0, rowCount, _leftTableIndexed)) {
        SQL_LOG(ERROR, "create hash table with right buffer failed.");
        return false;
    }
    uint64_t beginJoin = TimeUtility::currentTime();
    HashValues hashValues;
    if (!getHashValues(batch.table, batch.offset, batch.count, *_joinColumns, hashValues)) {
        return false;
    }
    reserveJoinRow(hashValues.size());
    size_t joinCount = 0;
    if (!_leftTableIndexed) {
        for (const auto &valuePair : hashValues) {
            auto iter = _hashJoinMap.find(valuePair.second);
            if (iter != _hashJoinMap.end()) {
                const auto &toJoinRows = iter->second;
                joinCount += toJoinRows.size();
                for (auto row : toJoinRows) {
                    joinRow(valuePair.first, row);
                }
            }
        }
    } else {
        for (const auto &valuePair : hashValues) {
            auto iter = _hashJoinMap.find(valuePair.second);
            if (iter != _hashJoinMap.end()) {
                const auto &toJoinRows = iter->second;
                joinCount += toJoinRows.size();
                for (auto row : toJoinRows) {
                    joinRow(row, valuePair.first);
                }
            }
        }
    }
    // joinCount is not excat, because there maybe duplicate rows
    _hasJoinedCount += joinCount;
    uint64_t afterJoin = TimeUtility::currentTime();
    JoinInfoCollector::incJoinTime(&_joinInfo, afterJoin - beginJoin);
    return true;
}

StreamQueryPtr LookupJoinKernel::genFinalStreamQuery(const LookupJoinBatch &batch) {
    StreamQueryPtr streamQuery;
    if (_scanInitParamR->tableType == SCAN_KV_TABLE_TYPE
        || _scanInitParamR->tableType == SCAN_KKV_TABLE_TYPE
        || _scanInitParamR->tableType == SCAN_SUMMARY_TABLE_TYPE
        || _scanInitParamR->tableType == SCAN_EXTERNAL_TABLE_TYPE) {
        vector<string> pks;
        if (genStreamKeys(batch, pks)) {
            streamQuery.reset(new StreamQuery());
            streamQuery->primaryKeys = std::move(pks);
        }
    } else if (_scanInitParamR->tableType == SCAN_NORMAL_TABLE_TYPE) {
        QueryPtr query;
        std::string field;
        vector<docid_t> docIds;
        if (getDocIdField(*_joinColumns, *_lookupColumns, field)
            && genDocIds(batch, field, docIds)) {
            query.reset(new DocIdsQuery(docIds));
        } else {
            query = genTableQuery(batch);
        }
        if (nullptr != query) {
            streamQuery.reset(new StreamQuery());
            streamQuery->query = query;
        }
    } else {
        SQL_LOG(ERROR,
                "gen stream query not support table [%s] type [%s]",
                _scanInitParamR->tableName.c_str(),
                _scanInitParamR->tableType.c_str());
    }
    return streamQuery;
}

bool LookupJoinKernel::getDocIdField(const vector<string> &inputFields,
                                     const vector<string> &joinFields,
                                     string &field) {
    if (joinFields.size() != 1 || inputFields.size() != 1) {
        return false;
    }
    if (joinFields[0] == INNER_DOCID_FIELD_NAME) {
        field = inputFields[0];
        return true;
    }
    return false;
}

bool LookupJoinKernel::genDocIds(const LookupJoinBatch &batch,
                                 const std::string &field,
                                 vector<docid_t> &docIds) {
    auto joinColumn = batch.table->getColumn(field);
    if (joinColumn == nullptr) {
        SQL_LOG(WARN, "invalid join column name [%s]", field.c_str());
        return false;
    }
    auto schema = joinColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(WARN, "get left join column [%s] schema failed", field.c_str());
        return false;
    }
    auto vt = schema->getType();
    if (vt.isMultiValue() || vt.getBuiltinType() != matchdoc::BuiltinType::bt_int32) {
        SQL_LOG(ERROR, "inner docid join only support int32_t");
        return false;
    }
    auto columnData = joinColumn->getColumnData<int32_t>();
    if (unlikely(!columnData)) {
        SQL_LOG(ERROR, "impossible cast column data failed");
        return false;
    }
    docIds.reserve(batch.count);
    for (size_t i = 0; i < batch.count; ++i) {
        auto row = batch.table->getRow(i + batch.offset);
        docIds.emplace_back(columnData->get(row));
    }
    return true;
}

bool LookupJoinKernel::genStreamKeys(const LookupJoinBatch &batch, vector<string> &pks) {
    string triggerField;
    if (!getPkTriggerField(*_joinColumns, *_lookupColumns, *_lookupIndexInfos, triggerField)) {
        SQL_LOG(ERROR, "get pk trigger field failed");
        return false;
    }
    for (size_t i = 0; i < batch.count; ++i) {
        auto row = batch.table->getRow(i + batch.offset);
        if (!genStreamQueryTerm(batch.table, row, triggerField, pks)) {
            SQL_LOG(ERROR, "found %s failed", triggerField.c_str());
            return false;
        }
    }
    return true;
}

bool LookupJoinKernel::getPkTriggerField(const vector<string> &inputFields,
                                         const vector<string> &joinFields,
                                         const map<string, sql::IndexInfo> &indexInfoMap,
                                         string &triggerField) {
    for (const auto &kv : indexInfoMap) {
        const auto &indexInfo = kv.second;
        if ("primary_key" != indexInfo.type && "primarykey64" != indexInfo.type
            && "primarykey128" != indexInfo.type) {
            continue;
        }
        for (size_t i = 0; i < joinFields.size(); ++i) {
            if (indexInfo.name != joinFields[i]) {
                continue;
            }
            if (unlikely(i >= inputFields.size())) {
                return false;
            }
            triggerField = inputFields[i];
            return true;
        }
        return false;
    }
    return false;
}

bool LookupJoinKernel::genStreamQueryTerm(const TablePtr &inputTable,
                                          Row row,
                                          const string &inputField,
                                          vector<string> &termVec) {
    // todo optimize for down
    auto joinColumn = inputTable->getColumn(inputField);
    if (joinColumn == nullptr) {
        SQL_LOG(WARN, "invalid join column name [%s]", inputField.c_str());
        return false;
    }
    auto schema = joinColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(WARN, "get left join column [%s] schema failed", inputField.c_str());
        return false;
    }
    auto vt = schema->getType();
    bool isMulti = vt.isMultiValue();
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        if (isMulti) {                                                                             \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                              \
            auto columnData = joinColumn->getColumnData<T>();                                      \
            if (unlikely(!columnData)) {                                                           \
                SQL_LOG(ERROR, "impossible cast column data failed");                              \
                return false;                                                                      \
            }                                                                                      \
            const auto &datas = columnData->get(row);                                              \
            for (size_t k = 0; k < datas.size(); k++) {                                            \
                termVec.emplace_back(StringUtil::toString(datas[k]));                              \
            }                                                                                      \
        } else {                                                                                   \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                             \
            auto columnData = joinColumn->getColumnData<T>();                                      \
            if (unlikely(!columnData)) {                                                           \
                SQL_LOG(ERROR, "impossible cast column data failed");                              \
                return false;                                                                      \
            }                                                                                      \
            const auto &data = columnData->get(row);                                               \
            termVec.emplace_back(StringUtil::toString(data));                                      \
        }                                                                                          \
        break;                                                                                     \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
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
    iter = hintsMap.find("lookupDisableCacheFields");
    if (iter != hintsMap.end()) {
        auto fields = StringUtil::split(iter->second, ",");
        _disableCacheFields.insert(fields.begin(), fields.end());
    }
    iter = hintsMap.find("lookupEnableRowDeduplicate");
    if (iter != hintsMap.end()) {
        _enableRowDeduplicate = (iter->second == "yes");
    }
    iter = hintsMap.find("disableMatchRowIndexOptimize");
    if (iter != hintsMap.end()) {
        _disableMatchRowIndexOptimize = (iter->second == "yes");
    }
}

bool LookupJoinKernel::selectValidField(const TablePtr &input,
                                        vector<string> &inputFields,
                                        vector<string> &joinFields,
                                        bool enableRowDeduplicate) {
    size_t inputFieldsCount = inputFields.size();
    size_t multiTermColCount = 0;
    size_t firstIndex = 0;
    for (size_t i = 0; i < inputFieldsCount; ++i) {
        auto col = input->getColumn(inputFields[i]);
        if (!col) {
            SQL_LOG(WARN, "get join column [%s] column failed", inputFields[i].c_str());
            return false;
        }
        auto schema = col->getColumnSchema();
        if (!schema) {
            SQL_LOG(WARN, "get join column [%s] schema failed", inputFields[i].c_str());
            return false;
        }
        bool isMultiValue = schema->getType().isMultiValue();
        if (isMultiValue) {
            ++multiTermColCount;
            if (multiTermColCount > 1 && enableRowDeduplicate) {
                SQL_LOG(ERROR, "do not support multi-multiTermColumn case");
                return false;
            }
        }
        if (enableRowDeduplicate == isMultiValue && firstIndex == 0 && i > 0) {
            firstIndex = i;
        }
    }
    if (firstIndex > 0) {
        swap(inputFields[firstIndex], inputFields[0]);
        swap(joinFields[firstIndex], joinFields[0]);
    }
    joinFields.resize(inputFieldsCount);
    return true;
}

ColumnTerm *LookupJoinKernel::makeColumnTerm(const LookupJoinBatch &batch,
                                             const string &inputField,
                                             const string &joinField,
                                             bool &returnEmptyQuery) {
    unique_ptr<ColumnTerm> columnTerm;
    size_t rowCount = batch.count;
    auto col = batch.table->getColumn(inputField);
    auto schema = col->getColumnSchema();
    bool enableCache = (_disableCacheFields.count(joinField) == 0);
    auto SingleTermFunc = [&](auto a) -> bool {
        using T = typename decltype(a)::value_type;
        auto p = new ColumnTermTyped<T>(joinField, enableCache);
        columnTerm.reset(p);
        auto &values = p->getValues();
        values.reserve(rowCount);
        auto colData = col->getColumnData<T>();
        if (unlikely(!colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            columnTerm.reset();
            return false;
        }
        for (size_t i = 0; i < batch.count; ++i) {
            const auto &row = batch.table->getRow(i + batch.offset);
            const auto &data = colData->get(row);
            values.push_back(data);
        }
        return true;
    };
    auto MultiTermFunc = [&](auto a) {
        using Multi = typename decltype(a)::value_type;
        using Single = typename Multi::value_type;
        auto p = new ColumnTermTyped<Single>(joinField, enableCache);
        columnTerm.reset(p);
        auto &values = p->getValues();
        auto &offsets = p->getOffsets();
        values.reserve(rowCount * 4);
        offsets.reserve(rowCount + 1);
        offsets.push_back(0);
        auto colData = col->getColumnData<Multi>();
        if (unlikely(!colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            columnTerm.reset();
            return false;
        }
        for (size_t i = 0; i < batch.count; ++i) {
            const auto &row = batch.table->getRow(i + batch.offset);
            const auto &datas = colData->get(row);
            auto dataSize = datas.size();
            for (size_t j = 0; j < dataSize; ++j) {
                values.push_back(datas[j]);
            }
            offsets.push_back(values.size());
        }
        if (values.empty()) {
            returnEmptyQuery = true;
        }
        return true;
    };
    if (!ValueTypeSwitch::switchType(schema->getType(), SingleTermFunc, MultiTermFunc)) {
        SQL_LOG(ERROR, "gen ColumnTerm failed");
        return nullptr;
    }
    return columnTerm.release();
}

void LookupJoinKernel::fillTerms(const LookupJoinBatch &batch, vector<ColumnTerm *> &terms) {
    bool returnEmptyQuery = false;
    for (uint16_t i = 0; i < _joinColumns->size(); i++) {
        if (_lookupIndexInfos->find((*_lookupColumns)[i]) == _lookupIndexInfos->end()) {
            continue;
        }
        ColumnTerm *term
            = makeColumnTerm(batch, (*_joinColumns)[i], (*_lookupColumns)[i], returnEmptyQuery);
        if (term) {
            terms.push_back(term);
        }
        if (returnEmptyQuery) {
            for (auto it : terms) {
                delete it;
            }
            terms.clear();
            return;
        }
    }

    if (!terms.empty() && _useMatchedRow) {
        auto &offsets = terms[0]->getOffsets();
        _rowMatchedInfo.clear();
        if (offsets.empty()) {
            _rowMatchedInfo.reserve(batch.count);
            for (size_t i = 0; i < batch.count; ++i) {
                _rowMatchedInfo.push_back({i + batch.offset});
            }
        } else {
            _rowMatchedInfo.reserve(*offsets.rbegin());
            for (size_t i = 0; i < batch.count; ++i) {
                _rowMatchedInfo.insert(
                    _rowMatchedInfo.end(), offsets[i + 1] - offsets[i], {i + batch.offset});
            }
        }
    }
}

QueryPtr LookupJoinKernel::genTableQuery(const LookupJoinBatch &batch) {
    if (!selectValidField(batch.table, *_joinColumns, *_lookupColumns, _enableRowDeduplicate)) {
        return QueryPtr();
    }
    vector<ColumnTerm *> terms;
    terms.reserve(_joinColumns->size());
    if (_enableRowDeduplicate) {
        fillTermsWithRowOptimized(batch, terms);
    } else {
        fillTerms(batch, terms);
    }
    TableQueryPtr tableQuery(new TableQuery(string()));
    if (!terms.empty()) {
        tableQuery->getTermArray() = terms;
    }
    return tableQuery;
}

bool LookupJoinKernel::genRowGroupKey(const LookupJoinBatch &batch,
                                      const vector<Column *> &singleTermCols,
                                      vector<string> &rowGroupKey) {
    size_t idx;
    auto MultiTermFunc = [&](auto a) { return false; };
    auto SingleTermFunc = [&](auto a) -> bool {
        using T = typename decltype(a)::value_type;
        auto colData = singleTermCols[idx]->getColumnData<T>();
        if (unlikely(!colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            return false;
        }
        for (size_t i = 0; i < batch.count; ++i) {
            const auto &row = batch.table->getRow(i + batch.offset);
            const auto &data = colData->get(row);
            rowGroupKey[i] += "#$" + StringUtil::toString(data);
        }
        return true;
    };
    for (idx = 0; idx < singleTermCols.size(); ++idx) {
        ValueType vt = singleTermCols[idx]->getColumnSchema()->getType();
        if (!ValueTypeSwitch::switchType(vt, SingleTermFunc, MultiTermFunc)) {
            SQL_LOG(ERROR, "gen single term hash value failed");
            return false;
        }
    }
    return true;
}

bool LookupJoinKernel::genSingleTermColumns(const TablePtr &input,
                                            const vector<Column *> &singleTermCols,
                                            const vector<uint16_t> &singleTermID,
                                            const unordered_map<string, vector<size_t>> &rowGroups,
                                            size_t rowCount,
                                            vector<ColumnTerm *> &terms) {
    size_t idx;
    auto MultiTermFunc = [&](auto a) { return false; };
    auto SingleTermFunc = [&](auto a) -> bool {
        using T = typename decltype(a)::value_type;
        auto colData = singleTermCols[idx]->getColumnData<T>();
        if (unlikely(!colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            return false;
        }
        const string &fieldName = (*_lookupColumns)[singleTermID[idx]];
        bool enableCache = (_disableCacheFields.count(fieldName) == 0);
        auto p = new ColumnTermTyped<T>(fieldName, enableCache);
        auto &values = p->getValues();
        values.reserve(rowCount);
        for (const auto &m : rowGroups) {
            const auto &row = input->getRow(m.second[0]);
            const auto &data = colData->get(row);
            values.push_back(data);
        }
        terms.push_back(p);
        return true;
    };
    for (idx = 0; idx < singleTermCols.size(); ++idx) {
        auto type = singleTermCols[idx]->getColumnSchema()->getType();
        if (!ValueTypeSwitch::switchType(type, SingleTermFunc, MultiTermFunc)) {
            SQL_LOG(ERROR, "gen SingleTermColumns failed");
            return false;
        }
    }
    return true;
}

bool LookupJoinKernel::genMultiTermColumns(const TablePtr &input,
                                           const vector<Column *> &multiTermCols,
                                           const vector<uint16_t> &multiTermID,
                                           const unordered_map<string, vector<size_t>> &rowGroups,
                                           size_t rowCount,
                                           vector<ColumnTerm *> &terms) {
    auto SingleTermFunc = [&](auto a) { return false; };
    auto MultiTermFunc = [&](auto a) {
        using Multi = typename decltype(a)::value_type;
        using Single = typename Multi::value_type;
        auto colData = multiTermCols[0]->getColumnData<Multi>();
        if (unlikely(!colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            return false;
        }
        const string &fieldName = (*_lookupColumns)[multiTermID[0]];
        bool enableCache = (_disableCacheFields.count(fieldName) == 0);
        auto p = new ColumnTermTyped<Single>(fieldName, enableCache);
        auto &values = p->getValues();
        auto &offsets = p->getOffsets();
        values.reserve(rowCount * 4);
        offsets.reserve(rowGroups.size() + 1);
        offsets.push_back(0);
        _rowMatchedInfo.clear();
        _rowMatchedInfo.reserve(values.size());
        for (const auto &m : rowGroups) {
            unordered_map<Single, vector<size_t>> value2rowId;
            for (auto i : m.second) {
                const auto &row = input->getRow(i);
                const auto &datas = colData->get(row);
                for (size_t j = 0; j < datas.size(); ++j) {
                    value2rowId[datas[j]].push_back(i);
                }
            }
            for (auto &pp : value2rowId) {
                values.push_back(pp.first);
                if (_useMatchedRow) {
                    _rowMatchedInfo.push_back(pp.second);
                }
            }
            offsets.push_back(values.size());
        }
        if (values.empty()) {
            delete p;
            return false;
        }
        terms.push_back(p);
        return true;
    };
    if (!multiTermCols.empty()) {
        ValueType vt = multiTermCols[0]->getColumnSchema()->getType();
        if (!ValueTypeSwitch::switchType(vt, SingleTermFunc, MultiTermFunc)) {
            SQL_LOG(ERROR, "gen MultiTermColumns failed");
            return false;
        }
    }
    return true;
}

void LookupJoinKernel::fillTermsWithRowOptimized(const LookupJoinBatch &batch,
                                                 vector<ColumnTerm *> &terms) {
    vector<Column *> singleTermCols, multiTermCols;
    vector<uint16_t> singleTermID, multiTermID;
    for (uint16_t i = 0; i < _joinColumns->size(); i++) {
        if (_lookupIndexInfos->find((*_lookupColumns)[i]) == _lookupIndexInfos->end()) {
            continue;
        }
        auto p = batch.table->getColumn((*_joinColumns)[i]);
        if (p->getColumnSchema()->getType().isMultiValue()) {
            multiTermCols.push_back(p);
            multiTermID.push_back(i);
        } else {
            singleTermCols.push_back(p);
            singleTermID.push_back(i);
        }
    }
    size_t rowCount = batch.table->getRowCount();
    vector<string> rowGroupKey(batch.count);
    unordered_map<string, vector<size_t>> rowGroups;
    if (genRowGroupKey(batch, singleTermCols, rowGroupKey)) {
        for (size_t i = 0; i < rowGroupKey.size(); ++i) {
            rowGroups[rowGroupKey[i]].push_back(i + batch.offset);
        }
        if (genMultiTermColumns(
                batch.table, multiTermCols, multiTermID, rowGroups, rowCount, terms)) {
            genSingleTermColumns(
                batch.table, singleTermCols, singleTermID, rowGroups, rowCount, terms);
        }
    }
    if (multiTermCols.empty() && _useMatchedRow) {
        _rowMatchedInfo.clear();
        _rowMatchedInfo.reserve(rowGroups.size());
        for (auto &p : rowGroups) {
            _rowMatchedInfo.push_back(p.second);
        }
    }
}

bool LookupJoinKernel::isDocIdsOptimize() {
    auto normalScan = dynamic_cast<NormalScanR *>(_scanBase);
    if (normalScan != nullptr) {
        SQL_LOG(DEBUG, "scan table is normal scan");
        return normalScan->isDocIdsOptimize();
    }
    return false;
}

REGISTER_KERNEL(LookupJoinKernel);

} // namespace sql
