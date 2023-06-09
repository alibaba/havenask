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
#include "ha3/sql/ops/scan/NormalScan.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/ObjectPool.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/calc/CalcWrapper.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/condition/InnerDocidExpression.h"
#include "ha3/sql/ops/scan/DocIdsScanIterator.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanIterator.h"
#include "ha3/sql/ops/scan/ScanIteratorCreator.h"
#include "ha3/sql/ops/scan/ScanResource.h"
#include "ha3/sql/ops/scan/ScanUtil.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/ObjectPoolResource.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/SubDocAccessor.h"
#include "matchdoc/ValueType.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/provider/common.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxParser.h"
#include "suez/turing/expression/util/AttributeInfos.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/LegacyIndexInfoHelper.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TypeTransformer.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;
using namespace isearch::search;
using namespace isearch::common;
using namespace indexlib::index;
using namespace kmonitor;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, NormalScan);

class InvertedIndexMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_dictBlockCacheReadLat, "NormalScan.dictionary.blockCacheReadLatency");
        REGISTER_GAUGE_MUTABLE_METRIC(_postingBlockCacheReadLat, "NormalScan.posting.blockCacheReadLatency");
        REGISTER_LATENCY_MUTABLE_METRIC(_searchedSegmentCount, "NormalScan.searchedSegmentCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_searchedInMemSegmentCount, "NormalScan.searchedInMemSegmentCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalDictLookupCount, "NormalScan.totalDictLookupCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalDictHitCount, "NormalScan.totalDictHitCount");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, InvertedIndexSearchTracer *metrics) {
        REPORT_MUTABLE_METRIC(_dictBlockCacheReadLat,
                              metrics->GetDictionaryBlockCacheCounter()->blockCacheReadLatency / 1000.0f);
        REPORT_MUTABLE_METRIC(_postingBlockCacheReadLat,
                              metrics->GetPostingBlockCacheCounter()->blockCacheReadLatency / 1000.0f);
        REPORT_MUTABLE_METRIC(_searchedSegmentCount, metrics->GetSearchedSegmentCount());
        REPORT_MUTABLE_METRIC(_searchedInMemSegmentCount, metrics->GetSearchedInMemSegmentCount());
        REPORT_MUTABLE_METRIC(_totalDictLookupCount, metrics->GetDictionaryLookupCount());
        REPORT_MUTABLE_METRIC(_totalDictHitCount, metrics->GetDictionaryHitCount());
    }

private:
    MutableMetric *_dictBlockCacheReadLat = nullptr;
    MutableMetric *_postingBlockCacheReadLat = nullptr;
    MutableMetric *_searchedSegmentCount = nullptr;
    MutableMetric *_searchedInMemSegmentCount = nullptr;
    MutableMetric *_totalDictLookupCount = nullptr;
    MutableMetric *_totalDictHitCount = nullptr;
};

NormalScan::NormalScan(const ScanInitParam &param)
    : ScanBase(param)
    , _nestTableJoinType(LEFT_JOIN)
    , _notUseMatchData(true)
    , _indexInfoHelper(nullptr)
    , _isDocIdsOptimize(false)
{
    // if (_param.scanResource.objectPoolResource) {
    //     _objectPoolReadOnly = _param.scanResource.objectPoolResource->getObjectPoolReadOnly();
    // }
}

NormalScan::~NormalScan() {
    reportInvertedMetrics(_tracerMap);
    _scanIter.reset();
    _attributeExpressionVec.clear();
    if (_indexInfoHelper) {
        POOL_DELETE_CLASS(_indexInfoHelper);
    }
}

void NormalScan::patchHintInfo(const std::map<std::string, std::string> &hintMap) {
    auto iter = hintMap.find("nestTableJoinType");
    if (iter != hintMap.end()) {
        if (iter->second == "inner") {
            _nestTableJoinType = INNER_JOIN;
        }
    }
    iter = hintMap.find("truncateDesc");
    if (iter != hintMap.end()) {
        _truncateDesc = std::move(iter->second);
    }
    iter = hintMap.find("MatchDataLabel");
    _matchDataLabel = "sub";
    if (iter != hintMap.end()) {
        if (iter->second == "full_term") {
            _matchDataLabel = "full_term";
        }
    }
#if defined(__coredumpha3__)
    iter = hintMap.find("coredump");
    if (iter != hintMap.end()) {
        int *a = nullptr;
        _limit = *a;
    }
#endif
}

bool NormalScan::doInit() {
    ScanIteratorCreatorParam iterParam;
    iterParam.analyzerFactory = _param.scanResource.analyzerFactory;
    iterParam.queryInfo = _param.scanResource.queryInfo;
    iterParam.tableName = _param.tableName;
    iterParam.auxTableName = _param.auxTableName;
    iterParam.indexPartitionReaderWrapper = _indexPartitionReaderWrapper;
    iterParam.attributeExpressionCreator = _attributeExpressionCreator;
    iterParam.matchDocAllocator = _matchDocAllocator;
    iterParam.tableInfo = _tableInfo;
    iterParam.pool = _param.scanResource.queryPool;
    iterParam.timeoutTerminator = _timeoutTerminator.get();
    iterParam.parallelIndex = _param.parallelIndex;
    iterParam.parallelNum = _param.parallelNum;
    iterParam.modelConfigMap = _modelConfigMap;
    iterParam.udfToQueryManager = _udfToQueryManager;
    iterParam.truncateDesc = _truncateDesc;
    iterParam.matchDataLabel = _matchDataLabel;
    iterParam.limit = _limit;
    iterParam.sortDesc = _param.sortDesc;
    iterParam.tableSortDescription = _param.scanResource.tableSortDescription;
    iterParam.forbidIndexs = &_param.forbidIndexs;
    _scanIterCreator.reset(new ScanIteratorCreator(iterParam));
    bool emptyScan = false;
    _baseCreateScanIterInfo.matchDataManager = _matchDataManager;
    if (!_param.matchType.empty()) {
        _matchDataManager->requireMatchData();
    }

    if (_param.matchType.count(SQL_MATCH_TYPE_FULL) > 0) {
        if (_batchSize == 0) {
            _batchSize = DEFAULT_BATCH_COUNT;
        }
        _indexPartitionReaderWrapper->setTopK(_batchSize + _param.reserveMaxCount);
        SQL_LOG(TRACE1, "set topk batchSize[%u] reserveMaxCount[%d]",
                _batchSize, _param.reserveMaxCount);
    }

    if (!_scanIterCreator->genCreateScanIteratorInfo(
                    _param.conditionJson, _param.indexInfos, _param.fieldInfos,
                    _baseCreateScanIterInfo))
    {
        SQL_LOG(WARN, "table [%s] generate create scan iterator info failed",
                _param.tableName.c_str());
        return false;
    }
    _scanIter = _scanIterCreator->createScanIterator(
            _baseCreateScanIterInfo, _useSub, emptyScan);
    auto *scanIter = _scanIter.get();
    SQL_LOG(TRACE3,
            "after create scan iterator[%p] type[%s]",
            scanIter,
            scanIter ? typeid(*scanIter).name() : "");
    if (_scanIter == NULL && !emptyScan) {
        SQL_LOG(WARN, "table [%s] create ha3 scan iterator failed", _param.tableName.c_str());
        return false;
    }
    if (!_param.matchType.empty()) {
        SQL_LOG(TRACE1, "require match data, match types [%s]",
                autil::StringUtil::toString(_param.matchType).c_str());
        requireMatchData();
    }
    if (!initOutputColumn()) {
        SQL_LOG(WARN, "table [%s] init scan column failed.", _param.tableName.c_str());
        return false;
    }
    if (_batchSize == 0) {
        uint32_t docSize = _matchDocAllocator->getDocSize();
        docSize += _matchDocAllocator->getSubDocSize();
        if (docSize == 0) {
            _batchSize = DEFAULT_BATCH_COUNT;
        } else {
            _batchSize = DEFAULT_BATCH_SIZE / docSize;
            if (_batchSize > (1 << 18)) { // max 256K
                _batchSize = 1 << 18;
            }
        }
    }
    return true;
}

bool NormalScan::postInitPushDownOp(PushDownOp &pushDownOp) {
    if (!_param.matchType.empty()) {
        auto *op = dynamic_cast<CalcWrapper *>(&pushDownOp);
        if (!op) {
            return true;
        }
        auto metaInfo = std::make_shared<MetaInfo>();
        _matchDataManager->getQueryTermMetaInfo(metaInfo.get());
        op->setMatchInfo(
            metaInfo, _indexInfoHelper, _indexPartitionReaderWrapper->getIndexReader());
    }
    return true;
}

void NormalScan::requireMatchData() {
    _matchDataManager->setQueryCount(1);
    std::unordered_set<std::string> matchTypeSet(
            _param.matchType.begin(), _param.matchType.end());

    if (matchTypeSet.count(SQL_MATCH_TYPE_SUB) != 0) {
        if (_matchDocAllocator->hasSubDocAllocator()) {
            SQL_LOG(DEBUG, "require sub simple match data");
            _matchDataManager->requireSubSimpleMatchData(
                    _matchDocAllocator.get(),
                    SIMPLE_MATCH_DATA_REF,
                    SUB_DOC_DISPLAY_GROUP,
                    _param.scanResource.queryPool);
            _notUseMatchData = false;
        } else {
            SQL_LOG(DEBUG, "require simple match data");
            _matchDataManager->requireSimpleMatchData(
                    _matchDocAllocator.get(),
                    SIMPLE_MATCH_DATA_REF,
                    SUB_DOC_DISPLAY_NO,
                    _param.scanResource.queryPool);
            _notUseMatchData = false;
        }
    } else if (matchTypeSet.count(SQL_MATCH_TYPE_SIMPLE) != 0) {
        SQL_LOG(DEBUG, "require simple match data");
        _matchDataManager->requireSimpleMatchData(
                _matchDocAllocator.get(),
                SIMPLE_MATCH_DATA_REF,
                SUB_DOC_DISPLAY_NO,
                _param.scanResource.queryPool);
        _notUseMatchData = false;
    }
    if (matchTypeSet.count(SQL_MATCH_TYPE_FULL) != 0) {
        SQL_LOG(DEBUG, "require full match data");
        _matchDataManager->requireMatchData(
                _matchDocAllocator.get(),
                MATCH_DATA_REF,
                SUB_DOC_DISPLAY_NO,
                _param.scanResource.queryPool);
        _notUseMatchData = false;
    }
    if (matchTypeSet.count(SQL_MATCH_TYPE_VALUE) != 0) {
        SQL_LOG(DEBUG, "require match value");
        _matchDataManager->requireMatchValues(
                _matchDocAllocator.get(),
                MATCH_VALUE_REF,
                SUB_DOC_DISPLAY_NO,
                _param.scanResource.queryPool);
        _notUseMatchData = false;
    }
    if (_functionProvider->getMatchInfoReader() == nullptr) {
        auto metaInfo = std::make_shared<suez::turing::MetaInfo>();
        _matchDataManager->getQueryTermMetaInfo(metaInfo.get());
        _functionProvider->initMatchInfoReader(std::move(metaInfo));
        _indexInfoHelper = POOL_NEW_CLASS(_param.scanResource.queryPool,
                LegacyIndexInfoHelper, _tableInfo->getIndexInfos());
        _functionProvider->setIndexInfoHelper(_indexInfoHelper);
        _functionProvider->setIndexReaderPtr(
                _indexPartitionReaderWrapper->getIndexReader());
    }
}

bool NormalScan::doBatchScan(TablePtr &table, bool &eof) {
    autil::ScopedTime2 seekTimer;
    vector<MatchDoc> matchDocs;
    eof = false;
    if (_scanIter != NULL && _seekCount < _limit) {
        uint32_t batchSize = _batchSize;
        if (_limit > _seekCount) {
            batchSize = std::min(_limit - _seekCount, batchSize);
        }
        matchDocs.reserve(batchSize);
        uint32_t lastScanCount = _scanIter->getTotalScanCount();
        auto r = _scanIter->batchSeek(batchSize, matchDocs);
        if (r.is_ok()) {
            eof = r.get();
        } else if (r.as<TimeoutError>()) {
            if (_param.scanResource.scanConfig.enableScanTimeout) {
                SQL_LOG(WARN,
                        "scan table [%s] timeout, info: [%s]",
                        _param.tableName.c_str(),
                        _scanInfo.ShortDebugString().c_str());
                eof = true;
            } else {
                SQL_LOG(ERROR,
                        "scan table [%s] timeout, abort, info: [%s]",
                        _param.tableName.c_str(),
                        _scanInfo.ShortDebugString().c_str());
                _scanIter.reset();
                return false;
            }
        } else {
            SQL_LOG(ERROR,
                    "scan table failed with error: [%s]",
                    r.get_error().get_stack_trace().c_str());
            _scanIter.reset();
            return false;
        }
        _seekCount += matchDocs.size();
        incTotalScanCount(_scanIter->getTotalScanCount() - lastScanCount);
        if (_seekCount >= _limit) {
            eof = true;
            uint32_t delCount = _seekCount - _limit;
            if (delCount > 0) {
                uint32_t reserveCount = matchDocs.size() - delCount;
                _matchDocAllocator->deallocate(matchDocs.data() + reserveCount, delCount);
                matchDocs.resize(reserveCount);
            }
        }
    } else {
        eof = true;
    }
    incSeekTime(seekTimer.done_us());

    autil::ScopedTime2 evaluteTimer;
    evaluateAttribute(matchDocs, _matchDocAllocator, _attributeExpressionVec, eof);
    incEvaluateTime(evaluteTimer.done_us());

    autil::ScopedTime2 outputTimer;
    bool reuseMatchDocAllocator = _pushDownMode || (eof && _scanOnce && _notUseMatchData) || (_param.sortDesc.topk != 0);
    table = createTable(matchDocs, _matchDocAllocator, reuseMatchDocAllocator);
    incOutputTime(outputTimer.done_us());

    if (eof && _scanIter) {
        _innerScanInfo.set_totalseekdoccount(
                _innerScanInfo.totalseekdoccount() + _scanIter->getTotalSeekDocCount());
        _innerScanInfo.set_usetruncate(_scanIter->useTruncate());
        _scanIter.reset();
    }
    getInvertedTracers(_tracerMap);
    collectInvertedInfos(_tracerMap);
    return true;
}

bool NormalScan::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
    _scanOnce = false;
    _scanIter.reset();
    if (nullptr == inputQuery) {
        return true;
    }
    const QueryPtr query = inputQuery->query;
    if (query == nullptr) {
        return true;
    }
    SQL_LOG(TRACE2, "update query [%s]", query->toString().c_str());
    CreateScanIteratorInfo createInfo = _baseCreateScanIterInfo;
    if (createInfo.query != nullptr) {
        QueryPtr andQuery(new AndQuery(""));
        andQuery->addQuery(query);
        andQuery->addQuery(createInfo.query);
        createInfo.query = andQuery;
    } else {
        createInfo.query = query;
    }
    SQL_LOG(TRACE2, "updated query [%s]", createInfo.query->toString().c_str());
    bool emptyScan = false;
    if (!_param.matchType.empty()) {
        _matchDataManager->requireMatchData();
    }
    if (_updateQueryCount != 0 && createInfo.filterWrapper != nullptr) {
        createInfo.filterWrapper->resetFilter();
    }
    _scanIter = _scanIterCreator->createScanIterator(createInfo, _useSub, emptyScan);
    auto *scanIter = _scanIter.get();
    SQL_LOG(TRACE3,
            "after create scan iterator[%p] type[%s]",
            scanIter,
            scanIter ? typeid(*scanIter).name() : "");
    _isDocIdsOptimize = dynamic_cast<DocIdsScanIterator *>(_scanIter.get()) != nullptr;
    if (_scanIter == NULL && !emptyScan) {
        SQL_LOG(WARN, "table [%s] create ha3 scan iterator failed", _param.tableName.c_str());
        return false;
    }

    _updateQueryCount++;
    if (!_param.matchType.empty()) {
        requireMatchData();
    }
    return true;
}

bool NormalScan::initOutputColumn() {
    _attributeExpressionVec.clear();
    _attributeExpressionVec.reserve(_param.outputFields.size());
    map<string, ExprEntity> exprsMap;
    unordered_map<string, string> renameMap;
    bool ret = ExprUtil::parseOutputExprs(
        _param.scanResource.queryPool, _param.outputExprsJson, exprsMap, renameMap);
    if (!ret) {
        SQL_LOG(WARN, "parse output exprs [%s] failed", _param.outputExprsJson.c_str());
        return false;
    }
    auto attributeInfos = _tableInfo->getAttributeInfos();
    map<string, pair<string, bool>> expr2Outputs;

    for (size_t i = 0; i < _param.outputFields.size(); i++) {
        const string &outputName = _param.outputFields[i];
        string outputFieldType = "";
        if (_param.outputFields.size() == _param.outputFieldsType.size()) {
            outputFieldType = _param.outputFieldsType[i];
        }
        string exprJson;
        string exprStr = outputName;
        AttributeExpression *expr = NULL;
        bool hasError = false;
        string errorInfo;
        auto iter = exprsMap.find(outputName);
        if (iter != exprsMap.end()) {
            exprJson = iter->second.exprJson;
            exprStr = iter->second.exprStr;
        }
        AutilPoolAllocator allocator(_param.scanResource.queryPool);
        SimpleDocument simpleDoc(&allocator);
        if (!exprJson.empty() && ExprUtil::parseExprsJson(exprJson, simpleDoc)
            && ExprUtil::isCaseOp(simpleDoc)) {
            if (copyField(exprJson, outputName, expr2Outputs)) {
                continue;
            }
            expr = ExprUtil::CreateCaseExpression(_attributeExpressionCreator,
                                                  outputFieldType,
                                                  simpleDoc,
                                                  hasError,
                                                  errorInfo,
                                                  _param.scanResource.queryPool);
            if (hasError || expr == NULL) {
                SQL_LOG(WARN, "create case expression failed [%s]", errorInfo.c_str());
                return false;
            }
        } else {
            if (copyField(exprStr, outputName, expr2Outputs)) {
                continue;
            }
            expr = initAttributeExpr(_param.tableName, outputName, outputFieldType, exprStr, expr2Outputs);
            if (!expr) {
                if (exprStr == outputName &&
                    _param.fieldInfos.find(outputName) != _param.fieldInfos.end() &&
                    attributeInfos->getAttributeInfo(outputName) == NULL) {
                    SQL_LOG(WARN,
                            "outputName [%s] not exist in attributes, use default null string expr",
                            outputName.c_str());
                    AtomicSyntaxExpr defaultSyntaxExpr(string("null"), vt_string, STRING_VALUE);
                    expr = ExprUtil::createConstExpression(
                        _param.scanResource.queryPool, &defaultSyntaxExpr, vt_string);
                    if (expr) {
                        _attributeExpressionCreator->addNeedDeleteExpr(expr);
                    }
                }
                if (!expr) {
                    SQL_LOG(ERROR,
                            "Create outputName [%s] expr [%s] failed.",
                            outputName.c_str(),
                            exprStr.c_str());
                    return false;
                }
            }
        }
        expr->setOriginalString(outputName);
        if (!expr->allocate(_matchDocAllocator.get())) {
            SQL_LOG(WARN,
                    "Create outputName [%s] expr [%s] failed.",
                    outputName.c_str(),
                    exprStr.c_str());
            return false;
        }
        auto refer = expr->getReferenceBase();
        refer->setSerializeLevel(SL_ATTRIBUTE);
        _attributeExpressionVec.push_back(expr);
    }
    _matchDocAllocator->extend();
    _matchDocAllocator->extendSub();
    return true;
}

bool NormalScan::copyField(const string &expr,
                           const string &outputName,
                           map<string, pair<string, bool>> &expr2Outputs) {
    auto iter = expr2Outputs.find(expr);
    if (iter != expr2Outputs.end()) {
        if (!iter->second.second) {
            _copyFieldMap[outputName] = iter->second.first;
            if (appendCopyColumn(iter->second.first, outputName, _matchDocAllocator)) {
                return true;
            }
        }
    } else {
        expr2Outputs[expr] = make_pair(outputName, false);
    }
    return false;
}

std::pair<SyntaxExpr*, bool> NormalScan::parseSyntaxExpr(const std::string& tableName, const std::string& exprStr) {
    SyntaxExpr *syntaxExpr = nullptr;
    bool needDelete = false;
    if (_objectPoolReadOnly) {
        std::string key = tableName + exprStr;
        syntaxExpr = _objectPoolReadOnly->get<SyntaxExpr>(key, [exprStr](){
            return SyntaxParser::parseSyntax(exprStr);});
    }

    if (!syntaxExpr) {
        syntaxExpr = SyntaxParser::parseSyntax(exprStr);
        needDelete = true;
    }

    return std::make_pair(syntaxExpr, needDelete);
}

suez::turing::AttributeExpression*
NormalScan::initAttributeExpr(const string& tableName, const string& outputName,
                              const string& outputFieldType, const string& exprStr,
                              map<string, pair<string, bool> >& expr2Outputs)
{
    if (exprStr == INNER_DOCID_FIELD_NAME) {
        auto expr = POOL_NEW_CLASS(_param.scanResource.queryPool, InnerDocIdExpression);
        SQL_LOG(TRACE2,"create inner docid expression [%s]", exprStr.c_str());
        if (expr != nullptr) {
            _attributeExpressionCreator->addNeedDeleteExpr(expr);
        }
        return expr;
    }

    auto exprResult = parseSyntaxExpr(tableName,exprStr);
    SyntaxExpr* syntaxExpr = exprResult.first;

    bool needDelete = exprResult.second;
    if (!syntaxExpr) {
        SQL_LOG(WARN, "parse syntax [%s] failed", exprStr.c_str());
        return NULL;
    }

    AttributeExpression *expr = NULL;
    if (syntaxExpr->getSyntaxExprType() == suez::turing::SYNTAX_EXPR_TYPE_CONST_VALUE) {
        auto iter2 = expr2Outputs.find(exprStr);
        if (iter2 != expr2Outputs.end()) {
            iter2->second.second = true;
        }
        AtomicSyntaxExpr *atomicExpr = dynamic_cast<AtomicSyntaxExpr *>(syntaxExpr);
        if (atomicExpr) {
            ExprResultType resultType = vt_unknown;
            const AttributeInfos *attrInfos = _tableInfo->getAttributeInfos();
            if (attrInfos) {
                const AttributeInfo *attrInfo = attrInfos->getAttributeInfo(outputName.c_str());
                if (attrInfo) {
                    FieldType fieldType = attrInfo->getFieldType();
                    resultType = TypeTransformer::transform(fieldType);
                }
            }
            if (resultType == vt_unknown) {
                if (outputFieldType != "") {
                    resultType = ExprUtil::transSqlTypeToVariableType(outputFieldType).first;
                } else {
                    AtomicSyntaxExprType type = atomicExpr->getAtomicSyntaxExprType();
                    if (type == FLOAT_VALUE) {
                        resultType = vt_float;
                    } else if (type == INTEGER_VALUE) {
                        resultType = vt_int32;
                    } else if (type == STRING_VALUE) {
                        resultType = vt_string;
                    }
                }
            }
            expr = ExprUtil::createConstExpression(
                _param.scanResource.queryPool, atomicExpr, resultType);
            if (expr) {
                SQL_LOG(TRACE2,
                        "create const expression [%s], result type [%d]",
                        exprStr.c_str(),
                        resultType)
                _attributeExpressionCreator->addNeedDeleteExpr(expr);
            }
        }
    }
    if (!expr) {
        expr = _attributeExpressionCreator->createAttributeExpression(syntaxExpr, true);
    }
    if (needDelete) {
        DELETE_AND_SET_NULL(syntaxExpr);
    }
    return expr;
}

bool NormalScan::appendCopyColumn(const std::string &srcField,
                                  const std::string &destField,
                                  MatchDocAllocatorPtr &matchDocAllocator) {
    auto srcRef = matchDocAllocator->findReferenceWithoutType(srcField);
    if (!srcRef) {
        SQL_LOG(WARN, "not found column [%s]", srcField.c_str());
        return false;
    }
    auto destRef = matchDocAllocator->cloneReference(
        srcRef, destField, matchDocAllocator->getDefaultGroupName());
    if (!destRef) {
        SQL_LOG(WARN, "clone column from [%s] to [%s] failed", srcField.c_str(), destField.c_str());
        return false;
    }
    return true;
}

void NormalScan::copyColumns(const map<string, string> &copyFieldMap,
                             const vector<MatchDoc> &matchDocVec,
                             MatchDocAllocatorPtr &matchDocAllocator) {
    for (auto iter : copyFieldMap) {
        auto srcRef = matchDocAllocator->findReferenceWithoutType(iter.second);
        if (!srcRef) {
            continue;
        }
        auto destRef = matchDocAllocator->findReferenceWithoutType(iter.first);
        if (!destRef) {
            continue;
        }
        for (auto matchDoc : matchDocVec) {
            if (destRef->isSubDocReference() && srcRef->isSubDocReference()) {
                auto accessor = matchDocAllocator->getSubDocAccessor();
                std::function<void(MatchDoc)> cloneSub
                    = [srcRef, destRef](MatchDoc newDoc) {
                          destRef->cloneConstruct(newDoc, newDoc, srcRef);
                      };
                accessor->foreach(matchDoc, cloneSub);
            } else {
                destRef->cloneConstruct(matchDoc, matchDoc, srcRef);
            }
        }
    }
}

void NormalScan::evaluateAttribute(vector<MatchDoc> &matchDocVec,
                                   MatchDocAllocatorPtr &matchDocAllocator,
                                   vector<AttributeExpression *> &attributeExpressionVec,
                                   bool eof) {
    ExpressionEvaluator<vector<AttributeExpression *>> evaluator(
        attributeExpressionVec, matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluateExpressions(matchDocVec.data(), matchDocVec.size());
    copyColumns(_copyFieldMap, matchDocVec, matchDocAllocator);
}

MatchDocAllocatorPtr NormalScan::copyMatchDocAllocator(vector<MatchDoc> &matchDocVec,
        const MatchDocAllocatorPtr &matchDocAllocator,
        bool reuseMatchDocAllocator,
        vector<MatchDoc> &copyMatchDocs) {
    MatchDocAllocatorPtr outputAllocator = matchDocAllocator;
    if (!reuseMatchDocAllocator) {
        outputAllocator.reset(matchDocAllocator->cloneAllocatorWithoutData());
        if (!matchDocAllocator->swapDocStorage(*outputAllocator, copyMatchDocs, matchDocVec))
        {
            SQL_LOG(ERROR, "clone table data failed");
            return nullptr;
        }
        // drop useless field group
        outputAllocator->reserveFields(SL_ATTRIBUTE);
    } else {
        copyMatchDocs.swap(matchDocVec);
    }
    return outputAllocator;
}

TablePtr NormalScan::doCreateTable(MatchDocAllocatorPtr outputAllocator,
                                   vector<MatchDoc> copyMatchDocs) {
    if (_useSub) {
        TablePtr table;
        flattenSub(outputAllocator, copyMatchDocs, table);
        CalcInitParam calcInitParam(_param.outputFields, _param.outputFieldsType);
        CalcResource calcResource;
        calcResource.memoryPoolResource = _param.scanResource.memoryPoolResource;
        calcResource.metricsReporter = _param.scanResource.metricsReporter;
        CalcTable calcTable(calcInitParam, std::move(calcResource));
        if (!calcTable.projectTable(table)) {
            SQL_LOG(WARN, "project table [%s] failed.", TableUtil::toString(table, 5).c_str());
        }
        return table;
    } else {
        TablePtr table(new Table(copyMatchDocs, outputAllocator));
        if (_param.sortDesc.topk != 0) {
            CalcInitParam calcInitParam(_param.outputFields, _param.outputFieldsType);
            CalcResource calcResource;
            calcResource.memoryPoolResource = _param.scanResource.memoryPoolResource;
            calcResource.metricsReporter = _param.scanResource.metricsReporter;
            CalcTable calcTable(calcInitParam, std::move(calcResource));
            if (!calcTable.projectTable(table)) {
                SQL_LOG(WARN, "sort project table [%s] failed.", TableUtil::toString(table, 5).c_str());
            }
        }
        return table;
    }
}

void NormalScan::flattenSub(MatchDocAllocatorPtr &outputAllocator,
                            const vector<MatchDoc> &copyMatchDocs,
                            TablePtr &table) {
    vector<MatchDoc> outputMatchDocs;
    auto accessor = outputAllocator->getSubDocAccessor();
    std::function<void(MatchDoc)> processNothing
        = [&outputMatchDocs](MatchDoc newDoc) { outputMatchDocs.push_back(newDoc); };
    auto firstSubRef = outputAllocator->getFirstSubDocRef();
    vector<MatchDoc> needConstructSubDocs;
    for (MatchDoc doc : copyMatchDocs) {
        MatchDoc &first = firstSubRef->getReference(doc);
        if (first != INVALID_MATCHDOC) {
            accessor->foreachFlatten(doc, outputAllocator.get(), processNothing);
        } else {
            if (_nestTableJoinType == INNER_JOIN) {
                continue;
            }
            // alloc empty sub doc
            MatchDoc subDoc = outputAllocator->allocateSub(doc);
            needConstructSubDocs.emplace_back(subDoc);
            outputMatchDocs.push_back(doc);
        }
    }
    if (!needConstructSubDocs.empty()) {
        ReferenceVector subRefs = outputAllocator->getAllSubNeedSerializeReferences(0);
        for (auto &ref : subRefs) {
            for (auto subDoc : needConstructSubDocs) {
                ref->construct(subDoc);
            }
        }
    }
    table.reset(new Table(outputMatchDocs, outputAllocator));
}

void NormalScan::getInvertedTracers(std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap) {
    auto invertedTracers = _indexPartitionReaderWrapper->getInvertedTracers();
    tracerMap.clear();
    auto *indexInfos = _tableInfo->getIndexInfos();
    for (const auto &pair : invertedTracers) {
        auto &indexName = *pair.first;
        auto *indexInfo = indexInfos->getIndexInfo(indexName.c_str());
        assert(indexInfo && "index info must exist");
        auto indexType = indexInfo->getIndexType();
        auto indexTypeStr = ScanUtil::indexTypeToString(indexType);
        auto *tracer = pair.second;
        SQL_LOG(TRACE3,
                "index[%s] with inverted tracer[%s]",
                indexName.c_str(),
                autil::StringUtil::toString(*tracer).c_str());
        auto &mergeTracer = tracerMap[indexTypeStr];
        mergeTracer += *tracer;
    }
}

void NormalScan::collectInvertedInfos(
    const std::map<std::string, InvertedIndexSearchTracer> &tracerMap) {
    auto *collector = _param.scanResource.searchInfoCollector;
    if (unlikely(!collector)) {
        return;
    }
    for (const auto &pair : tracerMap) {
        auto &indexTypeStr = pair.first;
        auto &tracer = pair.second;
        InvertedIndexSearchInfo indexInfoProto;
        collectBlockInfo(tracer.GetDictionaryBlockCacheCounter(),
                         *indexInfoProto.mutable_dictionaryinfo());
        collectBlockInfo(tracer.GetPostingBlockCacheCounter(),
                         *indexInfoProto.mutable_postinginfo());
        indexInfoProto.set_searchedsegmentcount(tracer.GetSearchedSegmentCount());
        indexInfoProto.set_searchedinmemsegmentcount(tracer.GetSearchedInMemSegmentCount());
        indexInfoProto.set_totaldictlookupcount(tracer.GetDictionaryLookupCount());
        indexInfoProto.set_totaldicthitcount(tracer.GetDictionaryHitCount());
        indexInfoProto.set_indextype(indexTypeStr);
        indexInfoProto.set_hashkey(
            autil::HashAlgorithm::hashString(indexTypeStr.c_str(), indexTypeStr.size(), 1));
        collector->overwriteInvertedInfo(indexInfoProto);
    }
}

void NormalScan::collectBlockInfo(const indexlib::util::BlockAccessCounter *counter, BlockAccessInfo &blockInfo) {
    blockInfo.set_blockcachehitcount(counter->blockCacheHitCount);
    blockInfo.set_blockcachemisscount(counter->blockCacheMissCount);
    blockInfo.set_blockcachereadlatency(counter->blockCacheReadLatency);
    blockInfo.set_blockcacheiocount(counter->blockCacheIOCount);
    blockInfo.set_blockcacheiodatasize(counter->blockCacheIODataSize);
}

void NormalScan::reportInvertedMetrics(
    std::map<std::string, InvertedIndexSearchTracer> &tracerMap) {
    auto *metricsReporter = _param.scanResource.metricsReporter;
    if (unlikely(!metricsReporter)) {
        return;
    }
    for (auto &pair : tracerMap) {
        auto &indexTypeStr = pair.first;
        auto *tracer = &pair.second;
        auto reporter = metricsReporter->getSubReporter(
            "", {{{"table_name", getTableNameForMetrics()}, {"index_type", indexTypeStr}}});
        reporter->report<InvertedIndexMetrics, InvertedIndexSearchTracer>(nullptr, tracer);
    }
}

} // namespace sql
} // namespace isearch
