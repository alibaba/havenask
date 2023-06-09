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
#include "ha3/sql/ops/scan/SummaryScan.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <memory>
#include <typeinfo>
#include <unordered_set>

#include "autil/CommonMacros.h"
#include "autil/ConstString.h"
#include "autil/ConstStringUtil.h"
#include "autil/DataBuffer.h"
#include "autil/LongHashValue.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/QueryTermVisitor.h"
#include "ha3/common/Term.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/condition/Condition.h"
#include "ha3/sql/ops/condition/ConditionParser.h"
#include "ha3/sql/ops/condition/ConditionVisitor.h"
#include "ha3/sql/ops/scan/AsyncSummaryLookupCallbackCtx.h"
#include "ha3/sql/ops/scan/PrimaryKeyScanConditionVisitor.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanResource.h"
#include "ha3/sql/ops/scan/ScanUtil.h"
#include "ha3/sql/ops/scan/SummaryScan.hpp"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/util/HashString.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/util/AttributeInfos.h"
#include "suez/turing/expression/util/FieldInfos.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/SummaryInfo.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TypeTransformer.h"
#include "table/ValueTypeSwitch.h"

using namespace std;
using namespace table;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;
using namespace indexlib::index;
using namespace isearch::common;
using namespace isearch::search;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, SummaryScan);

SummaryScan::SummaryScan(const ScanInitParam &param, navi::AsyncPipePtr pipe, bool requirePk)
    : ScanBase(param)
    , _usedFields(param.usedFields)
    , _asyncPipe(pipe)
    , _requirePk(requirePk) {}

SummaryScan::~SummaryScan() {}

bool SummaryScan::doInit() {
    const ScanResource &scanResource = _param.scanResource;
    if (scanResource.metricsReporter != nullptr) {
        _opMetricsReporter = scanResource.metricsReporter;
    }
    initSummary();
    initExtraSummary(scanResource.dependencyTableInfoMap,
                     scanResource.cavaPluginManager,
                     scanResource.functionInterfaceCreator,
                     scanResource.partitionReaderSnapshot);
    // init pk query
    std::vector<std::string> pks;
    bool needFilter = true;
    if (!parseQuery(pks, needFilter)) {
        SQL_LOG(ERROR, "parse query failed");
        return false;
    }
    SQL_LOG(TRACE2, "before filter rawPks size[%lu]", pks.size());
    SQL_LOG(TRACE3, "before filter rawPks[%s]", StringUtil::toString(pks).c_str());
    if (!genDocIdFromRawPks(std::move(pks))) {
        SQL_LOG(WARN, "generate docid failed");
        return false;
    }
    _summaryInfo = _tableInfo->getSummaryInfo();
    if (_summaryInfo == nullptr) {
        SQL_LOG(ERROR, "get summary info failed");
        return false;
    }
    if (!prepareLookupCtxs()) {
        SQL_LOG(ERROR, "prepare lookup context failed");
        return false;
    }
    startLookupCtxs();

    // sort usedFields
    if (_usedFields != _param.outputFields) {
        adjustUsedFields();
    }
    // prepare attr && _table declare column
    if (!prepareFields()) {
        SQL_LOG(ERROR, "prepare fields failed");
        return false;
    }
    // init calc
    CalcInitParam calcInitParam(
        _param.outputFields, _param.outputFieldsType, _param.conditionJson, _param.outputExprsJson);
    CalcResource calcResource;
    calcResource.memoryPoolResource = scanResource.memoryPoolResource;
    calcResource.tracer = scanResource.tracer;
    calcResource.cavaAllocator = scanResource.suezCavaAllocator;
    calcResource.cavaPluginManager = scanResource.cavaPluginManager;
    calcResource.funcInterfaceCreator = scanResource.functionInterfaceCreator;
    calcResource.metricsReporter = scanResource.metricsReporter;
    _calcTable.reset(new CalcTable(calcInitParam, std::move(calcResource)));
    // set if need filter
    _calcTable->setFilterFlag(needFilter);
    if (!_calcTable->init()) {
        SQL_LOG(ERROR, "calc table init failed");
        return false;
    }
    return true;
}

void SummaryScan::initSummary() {
    _indexPartitionReaderWrappers.emplace_back(_indexPartitionReaderWrapper);
    _attributeExpressionCreators.emplace_back(_attributeExpressionCreator);
}

void SummaryScan::initExtraSummary(
    const map<string, suez::turing::TableInfoPtr> *dependencyTableInfoMap,
    suez::turing::CavaPluginManager *cavaPluginManager,
    suez::turing::FunctionInterfaceCreator *functionInterfaceCreator,
    indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot) {
    if (dependencyTableInfoMap == nullptr) {
        return;
    }
    const string &extraTableName = _param.tableName + "_extra";
    suez::turing::TableInfoPtr tableInfo;
    auto iter = dependencyTableInfoMap->find(extraTableName);
    if (iter != dependencyTableInfoMap->end()) {
        tableInfo = iter->second;
    }
    if (!tableInfo) {
        SQL_LOG(TRACE1, "table info [%s] not found", extraTableName.c_str());
        return;
    }

    auto tabletReader = partitionReaderSnapshot->GetTabletReader(extraTableName);
    if (tabletReader) {
        _indexPartitionReaderWrappers.emplace_back(
            std::make_shared<IndexPartitionReaderWrapper>(tabletReader));
    } else {
        auto mainIndexPartReader = partitionReaderSnapshot->GetIndexPartitionReader(extraTableName);
        if (!mainIndexPartReader) {
            SQL_LOG(TRACE1, "table [%s] not found", extraTableName.c_str());
            return;
        }
        vector<indexlib::partition::IndexPartitionReaderPtr> *indexReaderVec
            = new vector<indexlib::partition::IndexPartitionReaderPtr>({mainIndexPartReader});
        _indexPartitionReaderWrappers.emplace_back(
            std::make_shared<IndexPartitionReaderWrapper>(nullptr, nullptr, indexReaderVec, true));
    }
    AttributeExpressionCreatorPtr attributeExpressionCreator(
        new AttributeExpressionCreator(_param.scanResource.queryPool,
                                       _matchDocAllocator.get(),
                                       extraTableName,
                                       partitionReaderSnapshot,
                                       tableInfo,
                                       {},
                                       functionInterfaceCreator,
                                       cavaPluginManager,
                                       _functionProvider.get(),
                                       tableInfo->getSubTableName()));
    _attributeExpressionCreators.emplace_back(attributeExpressionCreator);
}

void SummaryScan::adjustUsedFields() {
    if (_usedFields.size() == _param.outputFields.size()) {
        std::unordered_set<std::string> outputFieldsSet(_param.outputFields.begin(),
                                                        _param.outputFields.end());
        bool needSort = true;
        for (size_t i = 0; i < _usedFields.size(); ++i) {
            if (outputFieldsSet.count(_usedFields[i]) == 0) {
                needSort = false;
                break;
            }
        }
        if (needSort) {
            _usedFields = _param.outputFields;
        }
    }
}

void SummaryScan::onBatchScanFinish() {
    for (size_t i = 0; i < _lookupCtxs.size(); ++i) {
        bool success = _opMetricsReporter && _lookupCtxs[i]->tryReportMetrics(*_opMetricsReporter);
        if (!success) {
            SQL_LOG(TRACE3, "ignore ctx[%lu] report metrics", i);
        }
    }
}

bool SummaryScan::doBatchScan(table::TablePtr &table, bool &eof) {
    _scanInfo.set_totalscancount(_scanInfo.totalscancount() + _docIds.size());

    for (auto &ctx : _lookupCtxs) {
        incSeekTime(ctx->getSeekTime());
    }

    autil::ScopedTime2 outputTimer;
    auto matchDocs = _matchDocAllocator->batchAllocate(_docIds);
    if (!fillSummary(matchDocs)) {
        SQL_LOG(ERROR, "fill summary failed");
        return false;
    }
    if (!fillAttributes(matchDocs)) {
        SQL_LOG(ERROR, "fill attribute failed");
        return false;
    }
    table = createTable(matchDocs, _matchDocAllocator, _scanOnce);
    incOutputTime(outputTimer.done_us());

    autil::ScopedTime2 evaluateTimer;
    if (_calcTable && !_calcTable->calcTable(table)) {
        SQL_LOG(ERROR, "calc table failed");
        return false;
    }
    _seekCount += table->getRowCount();
    incEvaluateTime(evaluateTimer.done_us());

    outputTimer = {};
    if (_limit < _seekCount) {
        table->clearBackRows(min(_seekCount - _limit, (uint32_t)table->getRowCount()));
    }
    incOutputTime(outputTimer.done_us());

    eof = true;
    return true;
}

bool SummaryScan::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
    _calcTable->setFilterFlag(true);
    _scanOnce = false;
    std::unordered_set<std::string> keySet;
    std::vector<std::string> pks;
    if (inputQuery == nullptr) {
        // pass
        SQL_LOG(TRACE3, "raw pk is empty for null input query");
    } else {
        const std::vector<std::string> &inputPks = inputQuery->primaryKeys;
        for (size_t i = 0; i < inputPks.size(); ++i) {
            const std::string &pk = inputPks[i];
            if (keySet.insert(pk).second) {
                pks.emplace_back(pk);
            }
        }
    }
    if (!genDocIdFromRawPks(std::move(pks))) {
        SQL_LOG(WARN, "generate docid failed, query [%s]", inputQuery->toString().c_str());
        return false;
    }
    startLookupCtxs();
    return true;
}

bool SummaryScan::genDocIdFromRawPks(vector<string> pks) {
    ScanUtil::filterPksByParam(_param, _tableInfo->getPrimaryKeyIndexInfo(), pks);
    SQL_LOG(TRACE2, "after filter pks, size[%lu]", pks.size());
    SQL_LOG(TRACE3, "after filter pks, filterPks[%s]", StringUtil::toString(pks).c_str());
    if (!convertPK2DocId(pks)) {
        SQL_LOG(ERROR, "convert pk to docid failed");
        return false;
    }
    SQL_LOG(TRACE2,
            "after convert PK to DocId, size[%lu], docIds[%s]",
            _docIds.size(),
            StringUtil::toString(_docIds).c_str());
    return true;
}

bool SummaryScan::parseQuery(std::vector<std::string> &pks, bool &needFilter) {
    ConditionParser parser(_param.scanResource.kernelPoolPtr.get());
    ConditionPtr condition;
    if (!parser.parseCondition(_param.conditionJson, condition)) {
        SQL_LOG(ERROR,
                "table name [%s], parse condition [%s] failed",
                _param.tableName.c_str(),
                _param.conditionJson.c_str());
        return false;
    }
    if (condition == nullptr) {
        return !_requirePk;
    }
    if (!_tableInfo) {
        SQL_LOG(ERROR, "table name [%s], talble info is null", _param.tableName.c_str());
        return false;
    }
    auto indexInfo = _tableInfo->getPrimaryKeyIndexInfo();
    if (!indexInfo) {
        SQL_LOG(ERROR, "table name [%s], pk index info is null", _param.tableName.c_str());
        return false;
    }
    SummaryScanConditionVisitor visitor(indexInfo->fieldName, indexInfo->indexName);
    condition->accept(&visitor);
    CHECK_VISITOR_ERROR(visitor);
    if (!visitor.stealHasQuery()) {
        if (_requirePk) {
            string errorMsg;
            if (condition) {
                errorMsg = "condition: " + condition->toString();
            } else {
                errorMsg = "condition json: " + _param.conditionJson;
            }
            SQL_LOG(ERROR,
                    "summary table name [%s] need pk query, %s",
                    _param.tableName.c_str(),
                    errorMsg.c_str());
        }
        return !_requirePk;
    }
    pks = visitor.getRawKeyVec();
    needFilter = visitor.needFilter();
    return true;
}

bool SummaryScan::prepareFields() {
    auto attributeInfos = _tableInfo->getAttributeInfos();
    if (attributeInfos == nullptr) {
        SQL_LOG(ERROR, "get attribute infos failed");
        return false;
    }
    if (_usedFields.empty()) {
        SQL_LOG(ERROR, "used fields can not be empty");
        return false;
    }
    _tableAttributes.resize(_attributeExpressionCreators.size());
    std::vector<MatchDoc> emptyMatchDocs;
    const FieldInfos *fieldInfos = _tableInfo->getFieldInfos();
    if (fieldInfos == nullptr) {
        SQL_LOG(ERROR, "get field infos failed");
        return false;
    }
    for (const auto &usedField : _usedFields) {
        if (_summaryInfo->exist(usedField)) {
            auto fieldInfo = fieldInfos->getFieldInfo(usedField.data());
            if (fieldInfo == nullptr) {
                SQL_LOG(ERROR, "get field info[%s] failed", usedField.c_str());
                return false;
            }
            matchdoc::BuiltinType bt = TypeTransformer::transform(fieldInfo->fieldType);
            bool isMulti = fieldInfo->isMultiValue;
            auto func = [&](auto a) {
                typedef typename decltype(a)::value_type T;
                auto ref = _matchDocAllocator->declareWithConstructFlagDefaultGroup<T>(
                    usedField, matchdoc::ConstructTypeTraits<T>::NeedConstruct(), SL_ATTRIBUTE);
                return ref != nullptr;
            };
            if (!ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
                SQL_LOG(ERROR, "declare column[%s] failed", usedField.c_str());
                return false;
            }
        } else {
            auto attributeInfo = attributeInfos->getAttributeInfo(usedField);
            if (attributeInfo == nullptr) {
                SQL_LOG(WARN,
                        "field [%s] not exist in schema, use default null string expr",
                        usedField.c_str());
                for (size_t i = 0; i < _attributeExpressionCreators.size(); ++i) {
                    AtomicSyntaxExpr defaultSyntaxExpr(string("null"), vt_string, STRING_VALUE);
                    auto expr = ExprUtil::createConstExpression(
                        _param.scanResource.queryPool, &defaultSyntaxExpr, vt_string);
                    expr->setOriginalString(usedField);
                    _attributeExpressionCreators[i]->addNeedDeleteExpr(expr);
                    if (!expr->allocate(_matchDocAllocator.get())) {
                        SQL_LOG(WARN, "allocate null string expr failed");
                        return false;
                    }
                    auto ref = expr->getReferenceBase();
                    ref->setSerializeLevel(SL_ATTRIBUTE);
                    _tableAttributes[i].push_back(expr);
                    SQL_LOG(
                        DEBUG, "field [%s] not found, init with empty string", usedField.c_str());
                }
            } else {
                for (size_t i = 0; i < _attributeExpressionCreators.size(); ++i) {
                    auto attrExpr
                        = _attributeExpressionCreators[i]->createAttributeExpression(usedField);
                    if (attrExpr == nullptr) {
                        SQL_LOG(ERROR, "create attr expr [%s] failed", usedField.c_str());
                        return false;
                    }
                    if (!attrExpr->allocate(_matchDocAllocator.get())) {
                        SQL_LOG(WARN, "allocate attr expr [%s] failed", usedField.c_str());
                        return false;
                    }
                    auto ref = attrExpr->getReferenceBase();
                    ref->setSerializeLevel(SL_ATTRIBUTE);
                    _tableAttributes[i].push_back(attrExpr);
                    SQL_LOG(DEBUG,
                            "field [%s] is not summary type, init with attribute",
                            usedField.c_str());
                }
            }
        }
    }
    _matchDocAllocator->extend();
    return true;
}

bool SummaryScan::convertPK2DocId(const std::vector<string> &pks) {
    std::vector<indexlib::index::PrimaryKeyIndexReader *> pkIndexReaders;
    for (auto indexPartitionReaderWrapper : _indexPartitionReaderWrappers) {
        auto primaryKeyReader = indexPartitionReaderWrapper->getPrimaryKeyReader().get();
        if (!primaryKeyReader) {
            SQL_LOG(ERROR, "can not find primary key index");
            return false;
        }
        pkIndexReaders.emplace_back(primaryKeyReader);
    }
    int tableSize = pkIndexReaders.size();
    _docIds.clear();
    _tableIdx.clear();
    for (const auto &pk : pks) {
        for (int i = 0; i < tableSize; ++i) {
            docid_t docId = INVALID_DOCID;
            try {
                docId = pkIndexReaders[i]->Lookup(pk);
            } catch (...) {
                SQL_LOG(ERROR, "primary key index lookup fail for key %s", pk.c_str());
                return false;
            }
            if (docId != INVALID_DOCID) {
                _docIds.emplace_back(docId);
                _tableIdx.emplace_back(i);
                break;
            } else {
                SQL_LOG(DEBUG, "can not find primary key %s", pk.c_str());
            }
        }
    }
    SQL_LOG(TRACE3,
            "after convert pk to docIds [%s] tableIdx [%s]",
            StringUtil::toString(_docIds).c_str(),
            StringUtil::toString(_tableIdx).c_str());
    return true;
}

bool SummaryScan::prepareLookupCtxs() {
    _countedPipe.reset(
        new CountedAsyncPipe(_asyncPipe, _indexPartitionReaderWrappers.size()));
    for (auto indexPartitionReaderWrapper : _indexPartitionReaderWrappers) {
        auto summaryReader = indexPartitionReaderWrapper->getSummaryReader();
        if (!summaryReader) {
            SQL_LOG(ERROR, "can not find summary reader");
            return false;
        }
        _lookupCtxs.emplace_back(
            make_shared<AsyncSummaryLookupCallbackCtx>(_countedPipe,
                                                       summaryReader,
                                                       _param.scanResource.queryPoolPtr,
                                                       _param.scanResource.asyncIntraExecutor));
    }
    return true;
}

void SummaryScan::startLookupCtxs() {
    _countedPipe->reset();
    size_t fieldCount = _summaryInfo->getFieldCount();
    std::vector<std::vector<docid_t>> docIdsVec(_lookupCtxs.size());
    for (auto &docIds : docIdsVec) {
        docIds.reserve(_docIds.size());
    }
    for (size_t i = 0; i < _docIds.size(); ++i) {
        docIdsVec[_tableIdx[i]].emplace_back(_docIds[i]);
    }
    int64_t leftTime
        = _timeoutTerminator ? _timeoutTerminator->getLeftTime() : numeric_limits<int64_t>::max();
    for (size_t i = 0; i < _lookupCtxs.size(); ++i) {
        SQL_LOG(TRACE2,
                "lookupCtxs idx[%lu] have docIds[%s] count[%lu], leftTime[%ld]",
                i,
                StringUtil::toString(docIdsVec[i]).c_str(),
                docIdsVec[i].size(),
                leftTime);
        _lookupCtxs[i]->start(std::move(docIdsVec[i]), fieldCount, leftTime);
    }
}

bool SummaryScan::getSummaryDocs(SearchSummaryDocVecType &summaryDocs) {
    std::vector<pair<const indexlib::index::SearchSummaryDocVec *, size_t>> resIters;
    for (size_t i = 0; i < _lookupCtxs.size(); ++i) {
        if (_lookupCtxs[i]->hasError()) {
            SQL_LOG(ERROR,
                    "summary async scan has error, lookupCtxs idx[%lu], error desc [%s]",
                    i,
                    _lookupCtxs[i]->getErrorDesc().c_str());
            return false;
        }
        auto &res = _lookupCtxs[i]->getResult();
        resIters.emplace_back(make_pair(&res, 0));
    }
    summaryDocs.clear();
    summaryDocs.reserve(_docIds.size());
    for (size_t i = 0; i < _docIds.size(); ++i) {
        auto res = resIters[_tableIdx[i]].first;
        auto &idx = resIters[_tableIdx[i]].second;
        assert(idx <= res->size() && "index overflow");
        summaryDocs.push_back((*res)[idx++]);
    }
    return true;
}

bool SummaryScan::fillSummary(const vector<matchdoc::MatchDoc> &matchDocs) {
    SearchSummaryDocVecType summaryDocs;
    if (!getSummaryDocs(summaryDocs)) {
        return false;
    }
    for (size_t i = 0; i < _summaryInfo->getFieldCount(); ++i) {
        const std::string &fieldName = _summaryInfo->getFieldName(i);
        ReferenceBase *refBase = _matchDocAllocator->findReferenceWithoutType(fieldName);
        if (refBase == nullptr) {
            continue;
        }
        auto vt = refBase->getValueType();
        auto func = [&](auto a) {
            typedef typename decltype(a)::value_type T;
            Reference<T> *ref = dynamic_cast<Reference<T> *>(refBase);
            return this->fillSummaryDocs<T>(ref, summaryDocs, matchDocs, i);
        };
        if (!ValueTypeSwitch::switchType(vt, func, func)) {
            SQL_LOG(ERROR, "fill summary docs column[%s] failed", fieldName.c_str());
            return false;
        }
    }
    return true;
}

bool SummaryScan::fillAttributes(const std::vector<matchdoc::MatchDoc> &matchDocs) {
    vector<vector<matchdoc::MatchDoc>> tableMatchDocs(_tableAttributes.size());
    for (size_t i = 0; i < matchDocs.size(); ++i) {
        tableMatchDocs[_tableIdx[i]].emplace_back(matchDocs[i]);
    }
    for (size_t i = 0; i < tableMatchDocs.size(); ++i) {
        for (auto &attrExpr : _tableAttributes[i]) {
            if (!attrExpr->batchEvaluate(tableMatchDocs[i].data(), tableMatchDocs[i].size())) {
                SQL_LOG(ERROR,
                        "batch evaluate failed, attr expr [%s]",
                        attrExpr->getOriginalString().c_str());
                return false;
            }
        }
    }
    return true;
}

} // namespace sql
} // namespace isearch
