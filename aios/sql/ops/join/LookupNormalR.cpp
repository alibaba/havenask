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
#include "sql/ops/join/LookupNormalR.h"

#include "autil/MultiValueType.h"
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
#include "sql/ops/scan/NormalScanR.h"
#include "suez/turing/navi/QueryMemPoolR.h"
#include "table/ValueTypeSwitch.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace isearch::common;

namespace sql {

const std::string LookupNormalR::RESOURCE_ID = "lookup_normal_r";

LookupNormalR::LookupNormalR() {}
LookupNormalR::~LookupNormalR() {}

void LookupNormalR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

navi::ErrorCode LookupNormalR::init(navi::ResourceInitContext &ctx) {
    auto ec = LookupR::init(ctx);
    if (navi::EC_NONE != ec) {
        return ec;
    }
    patchLookupHintInfo();
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
        _matchedInfo = std::make_unique<isearch::search::MatchDataCollectMatchedInfo>(
            std::string(HA3_MATCHDATAFIELD_MATCHEDROWINFO), &_rowMatchedInfo);
        _matchedInfo->setMatchDocAllocator(_normalScanR->getMatchDocAllocator().get());
        _matchedInfo->setPool(_joinParamR->_queryMemPoolR->getPool().get());
        if (!_matchedInfo->init()) {
            SQL_LOG(ERROR,
                    "init matchDataCollectMatchedInfo failed, table [%s].",
                    _scanInitParamR->tableName.c_str());
            return navi::EC_ABORT;
        }
    }
    _isDocIdsOptimize = _normalScanR->isDocIdsOptimize();
    SQL_LOG(DEBUG,
            "init lookup_normal_r success, useMatchedRow[%d], isDocIdsOptimize[%d], "
            "enableRowDeduplicate[%d], disableMatchRowIndexOptimize[%d]",
            _useMatchedRow,
            _isDocIdsOptimize,
            _enableRowDeduplicate,
            _disableMatchRowIndexOptimize);
    return navi::EC_NONE;
}

void LookupNormalR::patchLookupHintInfo() {
    auto &hintsMap = _joinParamR->_joinHintMap;
    if (hintsMap.empty()) {
        return;
    }
    auto iter = hintsMap.find("lookupDisableCacheFields");
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

std::shared_ptr<StreamQuery> LookupNormalR::genFinalStreamQuery(const LookupJoinBatch &batch) {
    std::shared_ptr<StreamQuery> streamQuery;
    QueryPtr query;
    std::string field;
    vector<docid_t> docIds;
    if (getDocIdField(*_joinColumns, *_lookupColumns, field) && genDocIds(batch, field, docIds)) {
        query.reset(new DocIdsQuery(docIds));
    } else {
        query = genTableQuery(batch);
    }
    if (nullptr != query) {
        streamQuery.reset(new StreamQuery());
        streamQuery->query = query;
    }
    if (_matchedInfo) {
        auto matchDataManager = _normalScanR->getMatchDataManager();
        matchDataManager->reset();
        matchDataManager->getMatchDataCollectorCenter().subscribe(_matchedInfo.get());
    }
    return streamQuery;
}

bool LookupNormalR::doJoinTable(const LookupJoinBatch &batch, const table::TablePtr &streamOutput) {
    if (_isDocIdsOptimize) {
        if (!docIdsJoin(batch, streamOutput)) {
            SQL_LOG(ERROR, "unexpected docIds join failed");
            return false;
        }
    } else if (_useMatchedRow) {
        createMatchedRowIndexs(streamOutput);
    } else if (!doHashJoin(batch, streamOutput)) {
        return false;
    }
    return true;
}

bool LookupNormalR::genMultiTermColumns(const TablePtr &input,
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

void LookupNormalR::fillTermsWithRowOptimized(const LookupJoinBatch &batch,
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

ColumnTerm *LookupNormalR::makeColumnTerm(const LookupJoinBatch &batch,
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
        if constexpr (std::is_same_v<T, autil::HllCtx>) {
            return false;
        } else {
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
        }
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

void LookupNormalR::fillTerms(const LookupJoinBatch &batch, vector<ColumnTerm *> &terms) {
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

bool LookupNormalR::genRowGroupKey(const LookupJoinBatch &batch,
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

bool LookupNormalR::genSingleTermColumns(const TablePtr &input,
                                         const vector<Column *> &singleTermCols,
                                         const vector<uint16_t> &singleTermID,
                                         const unordered_map<string, vector<size_t>> &rowGroups,
                                         size_t rowCount,
                                         vector<ColumnTerm *> &terms) {
    size_t idx;
    auto MultiTermFunc = [&](auto a) { return false; };
    auto SingleTermFunc = [&](auto a) -> bool {
        using T = typename decltype(a)::value_type;
        if constexpr (std::is_same_v<T, autil::HllCtx>) {
            return false;
        } else {
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
        }
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

std::shared_ptr<isearch::common::Query> LookupNormalR::genTableQuery(const LookupJoinBatch &batch) {
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

bool LookupNormalR::docIdsJoin(const LookupJoinBatch &batch, const TablePtr &streamOutput) {
    SQL_LOG(TRACE3, "use docIds join optimize");
    if (batch.count != streamOutput->getRowCount()) {
        SQL_LOG(ERROR,
                "unexpected input table size [%lu] != joined table size [%lu]",
                batch.count,
                streamOutput->getRowCount());
        return false;
    }
    _joinParamR->reserveJoinRow(batch.count);
    for (size_t i = 0; i < batch.count; ++i) {
        _joinParamR->joinRow(i + batch.offset, i);
    }
    return true;
}

void LookupNormalR::createMatchedRowIndexs(const TablePtr &streamOutput) {
    SQL_LOG(TRACE3, "use matched info join optimize");
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
        _joinParamR->_tableAIndexes.swap(indexA);
        _joinParamR->_tableBIndexes.swap(indexB);
    } else {
        _joinParamR->_tableAIndexes.swap(indexB);
        _joinParamR->_tableBIndexes.swap(indexA);
    }
    _hasJoinedCount += _joinParamR->_tableAIndexes.size();
}

bool LookupNormalR::selectValidField(const TablePtr &input,
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

bool LookupNormalR::genDocIds(const LookupJoinBatch &batch,
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

bool LookupNormalR::getDocIdField(const vector<string> &inputFields,
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

REGISTER_RESOURCE(LookupNormalR);

} // namespace sql
