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
#include "sql/ops/join/LookupR.h"

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "matchdoc/ValueType.h"
#include "sql/common/IndexInfo.h"
#include "sql/common/TableMeta.h"
#include "sql/common/common.h"
#include "sql/ops/join/JoinBase.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/util/KernelUtil.h"

using namespace std;
using namespace autil;
using namespace table;

namespace sql {

const std::string LookupR::RESOURCE_ID = "lookup_r";

LookupR::LookupR() {}
LookupR::~LookupR() {}

void LookupR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool LookupR::config(navi::ResourceConfigContext &ctx) {
    // left table has index
    NAVI_JSONIZE(ctx, "left_is_build", _leftTableIndexed, _leftTableIndexed);

    TableMeta leftTableMeta, rightTableMeta;
    NAVI_JSONIZE(ctx, "left_table_meta", leftTableMeta, leftTableMeta);
    NAVI_JSONIZE(ctx, "right_table_meta", rightTableMeta, rightTableMeta);
    leftTableMeta.extractIndexInfos(_leftIndexInfos);
    rightTableMeta.extractIndexInfos(_rightIndexInfos);
    KernelUtil::stripName(_leftIndexInfos);
    KernelUtil::stripName(_rightIndexInfos);
    return true;
}

navi::ErrorCode LookupR::init(navi::ResourceInitContext &ctx) {
    if (_leftTableIndexed
        && (_joinParamR->_joinType == SQL_LEFT_JOIN_TYPE
            || _joinParamR->_joinType == SQL_ANTI_JOIN_TYPE)) {
        SQL_LOG(
            ERROR, "leftIsBuild is conflict with joinType[%s]!", _joinParamR->_joinType.c_str());
        return navi::EC_ABORT;
    }
    if (_leftTableIndexed) {
        _lookupColumns = &_joinParamR->_leftJoinColumns;
        _joinColumns = &_joinParamR->_rightJoinColumns;
        _lookupIndexInfos = &_leftIndexInfos;
    } else {
        _lookupColumns = &_joinParamR->_rightJoinColumns;
        _joinColumns = &_joinParamR->_leftJoinColumns;
        _lookupIndexInfos = &_rightIndexInfos;
    }
    return navi::EC_NONE;
}

std::shared_ptr<StreamQuery> LookupR::genFinalStreamQuery(const LookupJoinBatch &batch) {
    std::shared_ptr<StreamQuery> streamQuery;
    vector<string> pks;
    if (genStreamKeys(batch, pks)) {
        streamQuery.reset(new StreamQuery());
        streamQuery->primaryKeys = std::move(pks);
    }
    return streamQuery;
}

bool LookupR::genStreamKeys(const LookupJoinBatch &batch, vector<string> &pks) {
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

bool LookupR::joinTable(const LookupJoinBatch &batch,
                        const TablePtr &streamOutput,
                        TablePtr &outputTable) {
    if (!doJoinTable(batch, streamOutput)) {
        SQL_LOG(ERROR, "join table failed");
        return false;
    }
    const vector<size_t> &leftTableIndexs = _joinParamR->_tableAIndexes;
    const vector<size_t> &rightTableIndexs = _joinParamR->_tableBIndexes;
    const auto &leftTable = _leftTableIndexed ? streamOutput : batch.table;
    const auto &rightTable = _leftTableIndexed ? batch.table : streamOutput;
    if (!_joinParamR->_joinPtr->generateResultTable(leftTableIndexs,
                                                    rightTableIndexs,
                                                    leftTable,
                                                    rightTable,
                                                    leftTable->getRowCount(),
                                                    outputTable)) {
        SQL_LOG(ERROR, "generate result table failed");
        return false;
    }
    _joinParamR->_tableAIndexes.clear();
    _joinParamR->_tableBIndexes.clear();
    return true;
}

bool LookupR::createHashMap(const table::TablePtr &table, size_t offset, size_t count) {
    autil::ScopedTime2 hashTimer;
    HashJoinMapR::HashValues values;
    if (!_hashJoinMapR->getHashValues(table, offset, count, *_lookupColumns, values)) {
        SQL_LOG(ERROR, "get hash values failed");
        return false;
    }
    if (_leftTableIndexed) {
        _joinInfoR->incLeftHashCount(values.size());
    } else {
        _joinInfoR->incRightHashCount(values.size());
    }
    _joinInfoR->incHashTime(hashTimer.done_us());

    autil::ScopedTime2 createTimer;
    _hashJoinMapR->createHashMap(values);
    _joinInfoR->incHashMapSize(_hashJoinMapR->_hashJoinMap.size());
    _joinInfoR->incCreateTime(createTimer.done_us());
    return true;
}

bool LookupR::doHashJoin(const LookupJoinBatch &batch, const TablePtr &streamTable) {
    SQL_LOG(TRACE3, "use hash map join optimize");
    size_t rowCount = streamTable->getRowCount();
    if (rowCount == 0) {
        return true;
    }
    // todo: once per scan eof
    // create hashmap form left table, then calculate right table's hash values
    if (!createHashMap(streamTable, 0, rowCount)) {
        SQL_LOG(ERROR, "create hash map failed.");
        return false;
    }
    autil::ScopedTime2 timer;
    std::vector<std::pair<size_t, size_t>> hashValues;
    if (!_hashJoinMapR->getHashValues(
            batch.table, batch.offset, batch.count, *_joinColumns, hashValues)) {
        SQL_LOG(ERROR, "get hash values failed");
        return false;
    }
    size_t joinCount = 0;
    auto &hashMap = _hashJoinMapR->_hashJoinMap;
    for (const auto &valuePair : hashValues) {
        auto iter = hashMap.find(valuePair.second);
        if (iter != hashMap.end()) {
            const auto &toJoinRows = iter->second;
            joinCount += toJoinRows.size();
            for (auto row : toJoinRows) {
                if (_leftTableIndexed) {
                    _joinParamR->joinRow(row, valuePair.first);
                } else {
                    _joinParamR->joinRow(valuePair.first, row);
                }
            }
        }
    }
    // joinCount is not excat, because there maybe duplicate rows
    _hasJoinedCount += joinCount;
    _joinParamR->reserveJoinRow(hashValues.size());
    _joinInfoR->incJoinTime(timer.done_us());
    return true;
}

bool LookupR::finishJoin(const table::TablePtr &table,
                         size_t offset,
                         table::TablePtr &outputTable) {
    if (_leftTableIndexed) {
        return _joinParamR->_joinPtr->finish(table, table->getRowCount(), outputTable);
    } else {
        return true;
    }
}

bool LookupR::finishJoinEnd(const table::TablePtr &table,
                            size_t offset,
                            table::TablePtr &outputTable) {
    if (!_leftTableIndexed) {
        return _joinParamR->_joinPtr->finish(table, table->getRowCount(), outputTable);
    } else {
        return true;
    }
}

bool LookupR::getPkTriggerField(const vector<string> &inputFields,
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

bool LookupR::genStreamQueryTerm(const TablePtr &inputTable,
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
            typedef matchdoc::MatchDocBuiltinType2CppType<ft, true>::CppType T;                    \
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
            typedef matchdoc::MatchDocBuiltinType2CppType<ft, false>::CppType T;                   \
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

REGISTER_RESOURCE(LookupR);

} // namespace sql
