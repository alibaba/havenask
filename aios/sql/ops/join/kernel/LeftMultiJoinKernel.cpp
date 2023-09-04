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
#include "sql/ops/join/LeftMultiJoinKernel.h"

#include <algorithm>
#include <cstddef>
#include <engine/NaviConfigContext.h>
#include <ext/alloc_traits.h>
#include <memory>
#include <stdint.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/HashUtil.h"
#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "kmonitor/client/MetricLevel.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/join/JoinBase.h"
#include "sql/ops/join/JoinInfoCollector.h"
#include "sql/ops/scan/KVScanR.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace navi {
class KernelInitContext;
} // namespace navi

using namespace autil;
using namespace std;
using namespace table;
using namespace matchdoc;
using namespace kmonitor;

namespace sql {

AUTIL_DECLARE_AND_SETUP_LOGGER(sql, LeftMultiJoinKernel);

LeftMultiJoinKernel::LeftMultiJoinKernel() {}

LeftMultiJoinKernel::~LeftMultiJoinKernel() {
    reportMetrics();
}

void LeftMultiJoinKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.LeftMultiJoinKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .selectR(
            {KVScanR::RESOURCE_ID},
            [](navi::KernelConfigContext &ctx) {
                std::string tableType;
                auto newCtx = ctx.enter("build_node");
                NAVI_JSONIZE(newCtx, "table_type", tableType);
                if (tableType == SCAN_KV_TABLE_TYPE) {
                    return 0;
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
    "kv_require_pk" : false,
    "kv_async" : false
})json");
    ;
    JoinKernelBase::addDepend(builder);
}

bool LeftMultiJoinKernel::config(navi::KernelConfigContext &ctx) {
    if (!JoinKernelBase::config(ctx)) {
        return false;
    }
    NAVI_JSONIZE(ctx, "right_table_meta", _rightTableMeta, _rightTableMeta);
    _rightTableMeta.extractIndexInfos(_rightIndexInfos);
    return true;
}

navi::ErrorCode LeftMultiJoinKernel::init(navi::KernelInitContext &context) {
    auto ec = JoinKernelBase::init(context);
    if (ec != navi::EC_NONE) {
        return ec;
    }
    if (_leftJoinColumns.size() != 1 || _rightJoinColumns.size() != 1) {
        SQL_LOG(ERROR,
                "only support join one column, left join columns [%s], right join columns [%s]",
                autil::StringUtil::toString(_leftJoinColumns).c_str(),
                autil::StringUtil::toString(_rightJoinColumns).c_str());
        return navi::EC_ABORT;
    }
    _joinColumnName = _leftJoinColumns[0];
    if (!checkIndexInfo()) {
        SQL_LOG(ERROR, "right column [%s] need index", _rightJoinColumns[0].c_str());
        return navi::EC_ABORT;
    }

    return navi::EC_NONE;
}

bool LeftMultiJoinKernel::checkIndexInfo() const {
    for (const auto &kv : _rightIndexInfos) {
        const auto &indexInfo = kv.second;
        if ("primary_key" != indexInfo.type && "primarykey64" != indexInfo.type
            && "primarykey128" != indexInfo.type) {
            continue;
        }
        if (indexInfo.name == _rightJoinColumns[0]) {
            return true;
        }
    }
    return false;
}

navi::ErrorCode LeftMultiJoinKernel::compute(navi::KernelComputeContext &runContext) {
    JoinInfoCollector::incComputeTimes(&_joinInfo);
    uint64_t beginTime = autil::TimeUtility::currentTime();
    navi::PortIndex portIndex(0, navi::INVALID_INDEX);
    table::TablePtr inputTable;
    bool inputEof;
    if (!getLookupInput(runContext, portIndex, true, inputTable, inputEof)) {
        return navi::EC_ABORT;
    }

    bool eof = false;
    table::TablePtr outputTable;
    if (inputTable) {
        if (!doMultiJoin(inputTable, outputTable) || outputTable == nullptr) {
            SQL_LOG(ERROR, "do lookup join failed");
            return navi::EC_ABORT;
        }
    }

    uint64_t endTime = autil::TimeUtility::currentTime();
    JoinInfoCollector::incTotalTime(&_joinInfo, endTime - beginTime);

    if (inputEof || (inputTable == nullptr || inputTable->getRowCount() == 0)) {
        eof = true;
    }
    _sqlSearchInfoCollectorR->getCollector()->overwriteJoinInfo(_joinInfo);
    if (outputTable || eof) {
        TableDataPtr tableData(new TableData(outputTable));
        runContext.setOutput(portIndex, tableData, eof);
    }

    return navi::EC_NONE;
}

bool LeftMultiJoinKernel::doMultiJoin(const table::TablePtr &inputTable, table::TablePtr &output) {
    auto streamQuery = genStreamQuery(inputTable, _joinColumnName);
    if (streamQuery == nullptr) {
        SQL_LOG(ERROR,
                "generate stream query failed, table [%s], joinColumnName [%s]",
                TableUtil::rowToString(inputTable, 0).c_str(),
                _joinColumnName.c_str());
        return false;
    }
    uint64_t beginUpdateQuery = TimeUtility::currentTime();
    if (!_scanBase->updateScanQuery(streamQuery)) {
        SQL_LOG(ERROR, "update scan query failed, query [%s]", streamQuery->toString().c_str());
        return false;
    }
    uint64_t endUpdateQuery = TimeUtility::currentTime();
    JoinInfoCollector::incRightUpdateQueryTime(&_joinInfo, endUpdateQuery - beginUpdateQuery);

    TablePtr scanOutput;
    bool eof = false;
    while (!eof) {
        TablePtr streamOutput;
        uint64_t beginScan = TimeUtility::currentTime();
        if (!_scanBase->batchScan(streamOutput, eof)) {
            SQL_LOG(ERROR, "scan batch scan failed, query [%s]", streamQuery->toString().c_str());
            return false;
        }
        if (streamOutput == nullptr) {
            SQL_LOG(ERROR, "scan output empty, query [%s]", streamQuery->toString().c_str());
            return false;
        }
        incTotalRightInputTable(streamOutput->getRowCount());
        uint64_t endScan = TimeUtility::currentTime();
        JoinInfoCollector::incRightScanTime(&_joinInfo, endScan - beginScan);

        if (scanOutput == nullptr) {
            scanOutput = std::move(streamOutput);
        } else {
            scanOutput->merge(std::move(streamOutput));
        }
    }

    uint64_t beginJoin = TimeUtility::currentTime();
    if (!joinAndGather(inputTable, scanOutput, output)) {
        SQL_LOG(ERROR, "multiJoin failed");
        return false;
    }
    uint64_t afterJoin = TimeUtility::currentTime();
    JoinInfoCollector::incJoinTime(&_joinInfo, afterJoin - beginJoin);

    return true;
}

// join and gather
bool LeftMultiJoinKernel::joinAndGather(const table::TablePtr &leftTable,
                                        const table::TablePtr &rightTable,
                                        table::TablePtr &output) {
    if (!initJoinedTable(leftTable, rightTable, output)) {
        SQL_LOG(ERROR, "init joined table failed");
        return false;
    }

    std::unordered_map<size_t, size_t> hashJoinMap;
    if (!createHashMap(rightTable, _rightJoinColumns, hashJoinMap)) {
        SQL_LOG(ERROR, "create hash map failed");
        return false;
    }

    table::Column *joinColumn = nullptr;
    ValueType vt;
    if (!getColumnInfo(leftTable, _joinColumnName, joinColumn, vt)) {
        return false;
    }
    if (!vt.isMultiValue()) {
        SQL_LOG(ERROR, "multi join left join column must be multi value");
        return false;
    }

    vector<vector<size_t>> resultIndexes;
    resultIndexes.resize(leftTable->getRowCount());

    auto joinEmptyCount = 0;
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                                  \
        auto columnData = joinColumn->getColumnData<T>();                                          \
        if (unlikely(!columnData)) {                                                               \
            SQL_LOG(ERROR, "impossible cast column data failed");                                  \
            return false;                                                                          \
        }                                                                                          \
        for (size_t i = 0; i < leftTable->getRowCount(); i++) {                                    \
            const auto &datas = columnData->get(i);                                                \
            bool findKey = false;                                                                  \
            for (size_t k = 0; k < datas.size(); k++) {                                            \
                auto hashKey = HashUtil::calculateHashValue(datas[k]);                             \
                const auto &iter = hashJoinMap.find(hashKey);                                      \
                if (iter == hashJoinMap.end()) {                                                   \
                    resultIndexes[i].push_back(-1);                                                \
                } else {                                                                           \
                    findKey = true;                                                                \
                    resultIndexes[i].push_back(iter->second);                                      \
                }                                                                                  \
            }                                                                                      \
            if (!findKey) {                                                                        \
                joinEmptyCount++;                                                                  \
            }                                                                                      \
        }                                                                                          \
        break;                                                                                     \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        return false;
    }
    }

    if (!generateOutput(leftTable, rightTable, resultIndexes, output)) {
        SQL_LOG(ERROR, "generate output failed");
        return false;
    }

    return true;
}

template <typename T>
bool setColumnData(table::Column *oldColumn, table::Column *newColumn) {
    auto columnData = oldColumn->getColumnData<T>();
    if (columnData == nullptr) {
        return false;
    }
    auto newColumnData = newColumn->getColumnData<T>();
    if (newColumnData == nullptr) {
        return false;
    }
    for (size_t j = 0; j < oldColumn->getRowCount(); j++) {
        newColumnData->set(j, columnData->get(j));
    }
    return true;
}

template <BuiltinType ft>
bool setExpandColumnData(table::Column *oldColumn,
                         table::Column *newColumn,
                         const vector<vector<size_t>> &resultIndexes,
                         autil::mem_pool::Pool *pool,
                         size_t rowCount) {
    typedef typename MatchDocBuiltinType2CppType<ft, false>::CppType T;
    typedef typename MatchDocBuiltinType2CppType<ft, true>::CppType TT;
    auto columnData = oldColumn->getColumnData<T>();
    auto newColumnData = newColumn->getColumnData<TT>();
    if (columnData == nullptr || newColumnData == nullptr) {
        return false;
    }
    for (size_t j = 0; j < rowCount; j++) {
        if constexpr (ft == bt_string) {
            vector<autil::StringView> values;
            values.reserve(resultIndexes[j].size());
            for (const auto &idx : resultIndexes[j]) {
                if (idx == -1) {
                    values.push_back(string());
                } else {
                    const auto &value = columnData->get(idx);
                    values.emplace_back(value.data(), value.size());
                }
            }
            char *buf = autil::MultiValueCreator::createMultiStringBuffer(values, pool);
            autil::MultiString ms(buf);
            newColumnData->set(j, ms);
        } else {
            vector<T> values;
            values.reserve(resultIndexes[j].size());
            for (const auto &idx : resultIndexes[j]) {
                if (idx == -1) {
                    values.push_back(T());
                } else {
                    values.push_back(columnData->get(idx));
                }
            }
            char *buf = autil::MultiValueCreator::createMultiValueBuffer<T>(values, pool);
            autil::MultiValueType<T> mv(buf);
            newColumnData->set(j, mv);
        }
    }
    return true;
}

bool LeftMultiJoinKernel::generateOutput(const table::TablePtr &leftTable,
                                         const table::TablePtr &rightTable,
                                         const vector<vector<size_t>> &resultIndexes,
                                         table::TablePtr &outputTable) {
    size_t outputSize = _joinBaseParam._outputFields.size();
    size_t leftSize = min(_joinBaseParam._leftInputFields.size(), outputSize);
    outputTable->batchAllocateRow(leftTable->getRowCount());

    for (size_t i = 0; i < leftSize; i++) {
        const string &inputField = _joinBaseParam._leftInputFields[i];
        const string &outputField = _joinBaseParam._outputFields[i];
        auto newColumn = outputTable->getColumn(outputField);
        if (newColumn == nullptr) {
            SQL_LOG(ERROR, "get output table column [%s] failed", outputField.c_str());
            return false;
        }
        table::Column *tableColumn = nullptr;
        ValueType vt;
        if (!getColumnInfo(leftTable, inputField, tableColumn, vt)) {
            return false;
        }
        bool isMulti = vt.isMultiValue();
        switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        if (isMulti) {                                                                             \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                              \
            if (!setColumnData<T>(tableColumn, newColumn)) {                                       \
                SQL_LOG(ERROR, "set column data failed, input [%s]", inputField.c_str());          \
            }                                                                                      \
        } else {                                                                                   \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                             \
            if (!setColumnData<T>(tableColumn, newColumn)) {                                       \
                SQL_LOG(ERROR, "set column data failed, input [%s]", inputField.c_str());          \
            }                                                                                      \
        }                                                                                          \
        break;                                                                                     \
    }
            BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
        default: {
            SQL_LOG(ERROR, "impossible reach this branch, bt: %u", vt.getBuiltinType());
            return false;
        }
        }
#undef CASE_MACRO
    }

    for (size_t i = leftSize; i < outputSize; i++) {
        const string &inputField = _joinBaseParam._rightInputFields[i - leftSize];
        const string &outputField = _joinBaseParam._outputFields[i];
        auto newColumn = outputTable->getColumn(outputField);
        if (newColumn == nullptr) {
            SQL_LOG(ERROR, "get output table column [%s] failed", outputField.c_str());
            return false;
        }
        table::Column *tableColumn = nullptr;
        ValueType vt;
        if (!getColumnInfo(rightTable, inputField, tableColumn, vt)) {
            return false;
        }
        bool isMultiValue = vt.isMultiValue();
        if (isMultiValue) {
            SQL_LOG(ERROR,
                    "right table dont support multi value, input field [%s], output field [%s], "
                    "schema [%s]",
                    inputField.c_str(),
                    outputField.c_str(),
                    tableColumn->getColumnSchema()->toString().c_str());
            return false;
        }

        auto pool = outputTable->getMatchDocAllocator()->getPool();
        switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        if (!setExpandColumnData<ft>(                                                              \
                tableColumn, newColumn, resultIndexes, pool, leftTable->getRowCount())) {          \
            return false;                                                                          \
        }                                                                                          \
        break;                                                                                     \
    }
            BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        default: {
            SQL_LOG(ERROR, "impossible reach this branch, bt: %u", vt.getBuiltinType());
            return false;
        }
        }
    }

    return true;
}

bool LeftMultiJoinKernel::createHashMap(const table::TablePtr &table,
                                        const std::vector<std::string> &joinColumns,
                                        std::unordered_map<size_t, size_t> &hashJoinMap) {
    uint64_t beginHash = TimeUtility::currentTime();

    HashValues values;
    if (!getHashValues(table, 0, table->getRowCount(), joinColumns, values)) {
        return false;
    }
    JoinInfoCollector::incRightHashCount(&_joinInfo, values.size());
    uint64_t afterHash = TimeUtility::currentTime();
    JoinInfoCollector::incHashTime(&_joinInfo, afterHash - beginHash);
    for (const auto &valuePair : values) {
        hashJoinMap.insert({valuePair.second, valuePair.first});
    }
    JoinInfoCollector::incHashMapSize(&_joinInfo, hashJoinMap.size());
    uint64_t endHash = TimeUtility::currentTime();
    JoinInfoCollector::incCreateTime(&_joinInfo, endHash - afterHash);
    return true;
}

bool LeftMultiJoinKernel::initJoinedTable(const table::TablePtr &leftTable,
                                          const table::TablePtr &rightTable,
                                          table::TablePtr &outputTable) {
    outputTable.reset(new table::Table(_graphMemoryPoolR->getPool()));
    outputTable->mergeDependentPools(leftTable);
    outputTable->mergeDependentPools(rightTable);

    size_t outputSize = _joinBaseParam._outputFields.size();
    size_t leftSize = min(_joinBaseParam._leftInputFields.size(), outputSize);
    for (size_t i = 0; i < leftSize; i++) {
        const string &inputField = _joinBaseParam._leftInputFields[i];
        const string &outputField = _joinBaseParam._outputFields[i];
        if (!declearTableColumn(leftTable, outputTable, inputField, outputField, false)) {
            return false;
        }
    }

    for (size_t i = leftSize; i < outputSize; i++) {
        const string &inputField = _joinBaseParam._rightInputFields[i - leftSize];
        const string &outputField = _joinBaseParam._outputFields[i];
        if (!declearTableColumn(rightTable, outputTable, inputField, outputField, true)) {
            return false;
        }
    }

    outputTable->endGroup();
    return true;
}

bool LeftMultiJoinKernel::declearTableColumn(const table::TablePtr &inputTable,
                                             table::TablePtr &outputTable,
                                             const std::string &inputField,
                                             const std::string &outputField,
                                             bool needExpand) const {
    table::Column *tableColumn = nullptr;
    ValueType vt;
    if (!getColumnInfo(inputTable, inputField, tableColumn, vt)) {
        return false;
    }
    Column *column = outputTable->declareColumn(
        outputField, vt.getBuiltinType(), vt.isMultiValue() | needExpand);
    if (column == nullptr) {
        SQL_LOG(ERROR, "declare column [%s] failed", outputField.c_str());
        return false;
    }
    return true;
}

bool LeftMultiJoinKernel::getColumnInfo(const table::TablePtr &table,
                                        const string &field,
                                        table::Column *&tableColumn,
                                        ValueType &vt) const {
    tableColumn = table->getColumn(field);
    if (tableColumn == nullptr) {
        SQL_LOG(ERROR, "column [%s] not found in table", field.c_str());
        return false;
    }
    auto schema = tableColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get column [%s] schema failed", field.c_str());
        return false;
    }
    vt = schema->getType();
    return true;
}

StreamQueryPtr LeftMultiJoinKernel::genStreamQuery(const table::TablePtr &input,
                                                   const std::string &joinColumnName) {
    StreamQueryPtr streamQuery = std::make_shared<StreamQuery>();
    vector<string> pks;

    auto column = input->getColumn(joinColumnName);
    if (column == nullptr) {
        SQL_LOG(ERROR, "invalid join column name [%s]", joinColumnName.c_str());
        return nullptr;
    }
    auto schema = column->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get left join column [%s] schema failed", joinColumnName.c_str());
        return nullptr;
    }
    auto vt = schema->getType();
    if (!vt.isMultiValue()) {
        SQL_LOG(
            ERROR, "left join column must be multi value, schema [%s]", schema->toString().c_str());
        return nullptr;
    }

    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                                  \
        auto columnData = column->getColumnData<T>();                                              \
        if (unlikely(!columnData)) {                                                               \
            SQL_LOG(ERROR, "cast column data failed");                                             \
            return nullptr;                                                                        \
        }                                                                                          \
        for (size_t i = 0; i < column->getRowCount(); i++) {                                       \
            const auto &data = columnData->get(i);                                                 \
            for (size_t j = 0; j < data.size(); j++) {                                             \
                pks.emplace_back(StringUtil::toString(data[j]));                                   \
            }                                                                                      \
        }                                                                                          \
        break;                                                                                     \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        SQL_LOG(ERROR, "impossible reach this branch, bt: %u", vt.getBuiltinType());
        return nullptr;
    }
    }

    streamQuery->primaryKeys = std::move(pks);
    return streamQuery;
}

REGISTER_KERNEL(LeftMultiJoinKernel);

} // namespace sql
