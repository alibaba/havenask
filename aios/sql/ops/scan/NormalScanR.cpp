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
#include "sql/ops/scan/NormalScanR.h"

#include <algorithm>
#include <assert.h>
#include <engine/NaviConfigContext.h>
#include <functional>
#include <set>
#include <stddef.h>
#include <stdint.h>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/result/Result.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/util/cache/BlockAccessCounter.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/SubDocAccessor.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/ResourceMap.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/FieldInfo.h"
#include "sql/common/Log.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/ops/condition/InnerDocidExpression.h"
#include "sql/ops/scan/DocIdsScanIterator.h"
#include "sql/ops/scan/udf_to_query/ContainToQueryImpl.h"
#include "sql/ops/sort/SortInitParam.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/provider/MetaInfo.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxParser.h"
#include "suez/turing/expression/util/AttributeInfos.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TypeTransformer.h"
#include "suez/turing/navi/QueryMemPoolR.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

namespace sql {

class InvertedIndexMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_dictBlockCacheReadLat,
                                      "NormalScan.dictionary.blockCacheReadLatency");
        REGISTER_GAUGE_MUTABLE_METRIC(_postingBlockCacheReadLat,
                                      "NormalScan.posting.blockCacheReadLatency");
        REGISTER_LATENCY_MUTABLE_METRIC(_searchedSegmentCount, "NormalScan.searchedSegmentCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_searchedInMemSegmentCount,
                                        "NormalScan.searchedInMemSegmentCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalDictLookupCount, "NormalScan.totalDictLookupCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalDictHitCount, "NormalScan.totalDictHitCount");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags,
                indexlib::index::InvertedIndexSearchTracer *metrics) {
        REPORT_MUTABLE_METRIC(_dictBlockCacheReadLat,
                              metrics->GetDictionaryBlockCacheCounter()->blockCacheReadLatency
                                  / 1000.0f);
        REPORT_MUTABLE_METRIC(_postingBlockCacheReadLat,
                              metrics->GetPostingBlockCacheCounter()->blockCacheReadLatency
                                  / 1000.0f);
        REPORT_MUTABLE_METRIC(_searchedSegmentCount, metrics->GetSearchedSegmentCount());
        REPORT_MUTABLE_METRIC(_searchedInMemSegmentCount, metrics->GetSearchedInMemSegmentCount());
        REPORT_MUTABLE_METRIC(_totalDictLookupCount, metrics->GetDictionaryLookupCount());
        REPORT_MUTABLE_METRIC(_totalDictHitCount, metrics->GetDictionaryHitCount());
    }

private:
    kmonitor::MutableMetric *_dictBlockCacheReadLat = nullptr;
    kmonitor::MutableMetric *_postingBlockCacheReadLat = nullptr;
    kmonitor::MutableMetric *_searchedSegmentCount = nullptr;
    kmonitor::MutableMetric *_searchedInMemSegmentCount = nullptr;
    kmonitor::MutableMetric *_totalDictLookupCount = nullptr;
    kmonitor::MutableMetric *_totalDictHitCount = nullptr;
};

const std::string NormalScanR::RESOURCE_ID = "normal_scan_r";

NormalScanR::NormalScanR() {}

NormalScanR::~NormalScanR() {
    reportInvertedMetrics(_tracerMap);
}

void NormalScanR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL)
        .enableBuildR({CalcTableR::RESOURCE_ID, ScanPushDownR::RESOURCE_ID});
}

bool NormalScanR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "enable_scan_timeout", _enableScanTimeout, _enableScanTimeout);
    return true;
}

navi::ErrorCode NormalScanR::init(navi::ResourceInitContext &ctx) {
    if (!ScanBase::doInit()) {
        return navi::EC_ABORT;
    }
    if (!initPushDown(ctx)) {
        return navi::EC_ABORT;
    }
    _ctx = ctx;
    autil::ScopedTime2 initTimer;
    if (_scanInitParamR->matchType.count(SQL_MATCH_TYPE_FULL) > 0) {
        if (_batchSize == 0) {
            _batchSize = DEFAULT_BATCH_COUNT;
        }
        auto reserveMaxCount = _scanInitParamR->reserveMaxCount;
        if (_scanPushDownR) {
            auto pushDownValue = _scanPushDownR->getReserveMaxCount();
            if (pushDownValue > -1) {
                reserveMaxCount = pushDownValue;
            }
        }
        _attributeExpressionCreatorR->_indexPartitionReaderWrapper->setTopK(_batchSize
                                                                            + reserveMaxCount);
        SQL_LOG(TRACE1,
                "set topk batchSize[%u] reserveMaxCount[%d]",
                _batchSize,
                _scanInitParamR->reserveMaxCount);
    }
    bool emptyScan = false;
    _scanIter = _scanIteratorCreatorR->createScanIterator(emptyScan);
    auto *scanIter = _scanIter.get();
    SQL_LOG(TRACE3,
            "after create scan iterator[%p] type[%s]",
            scanIter,
            scanIter ? typeid(*scanIter).name() : "");
    _isDocIdsOptimize = dynamic_cast<DocIdsScanIterator *>(_scanIter.get()) != nullptr;
    if (_scanIter == NULL && !emptyScan) {
        SQL_LOG(
            WARN, "table [%s] create ha3 scan iterator failed", _scanInitParamR->tableName.c_str());
        return navi::EC_ABORT;
    }
    if (!initOutputColumn()) {
        SQL_LOG(WARN, "table [%s] init scan column failed.", _scanInitParamR->tableName.c_str());
        return navi::EC_ABORT;
    }
    if (_batchSize == 0) {
        const auto &matchDocAllocator = _attributeExpressionCreatorR->_matchDocAllocator;
        uint32_t docSize = matchDocAllocator->getDocSize();
        docSize += matchDocAllocator->getSubDocSize();
        if (docSize == 0) {
            _batchSize = DEFAULT_BATCH_COUNT;
        } else {
            _batchSize = DEFAULT_BATCH_SIZE / docSize;
            if (_batchSize > (1 << 18)) { // max 256K
                _batchSize = 1 << 18;
            }
        }
    }
    initNestTableJoinType();
    postInitPushDown();
    _scanInitParamR->incInitTime(initTimer.done_us());
    return navi::EC_NONE;
}

bool NormalScanR::initPushDown(navi::ResourceInitContext &ctx) {
    auto configContext = ctx.getConfigContext();
    if (!configContext.hasKey("push_down_ops")) {
        return true;
    }
    navi::ResourceMap inputResourceMap;
    _scanPushDownR = std::dynamic_pointer_cast<ScanPushDownR>(
        ctx.buildR(ScanPushDownR::RESOURCE_ID, ctx.getConfigContext(), inputResourceMap));
    if (!_scanPushDownR) {
        NAVI_KERNEL_LOG(ERROR, "build ScanPushDownR failed");
        return false;
    }
    setPushDownMode(_scanPushDownR.get());
    return true;
}

void NormalScanR::postInitPushDown() {
    if (!_scanPushDownR) {
        return;
    }
    const auto &matchDataManager = _scanIteratorCreatorR->getMatchDataManager();
    auto metaInfo = std::make_shared<suez::turing::MetaInfo>();
    matchDataManager->getQueryTermMetaInfo(metaInfo.get());
    _scanPushDownR->setMatchInfo(
        metaInfo,
        _scanIteratorCreatorR->getIndexInfoHelper(),
        _attributeExpressionCreatorR->_indexPartitionReaderWrapper->getIndexReader());
}

void NormalScanR::initNestTableJoinType() {
    auto scanHintMap = _scanInitParamR->getScanHintMap();
    if (!scanHintMap) {
        return;
    }
    const auto &hints = *scanHintMap;
    auto iter = hints.find("nestTableJoinType");
    if (iter != hints.end()) {
        if (iter->second == "inner") {
            _nestTableJoinType = INNER_JOIN;
        }
    }
#if defined(__coredumpha3__)
    iter = hints.find("coredump");
    if (iter != hints.end()) {
        int *a = nullptr;
        _limit = *a;
    }
#endif
}

bool NormalScanR::doBatchScan(table::TablePtr &table, bool &eof) {
    autil::ScopedTime2 seekTimer;
    std::vector<matchdoc::MatchDoc> matchDocs;
    eof = false;
    const auto &matchDocAllocator = _attributeExpressionCreatorR->_matchDocAllocator;
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
        } else if (r.as<isearch::common::TimeoutError>()) {
            if (_enableScanTimeout) {
                SQL_LOG(WARN,
                        "scan table [%s] timeout, info: [%s]",
                        _scanInitParamR->tableName.c_str(),
                        _scanInitParamR->scanInfo.ShortDebugString().c_str());
                eof = true;
            } else {
                SQL_LOG(ERROR,
                        "scan table [%s] timeout, abort, info: [%s]",
                        _scanInitParamR->tableName.c_str(),
                        _scanInitParamR->scanInfo.ShortDebugString().c_str());
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
        _scanInitParamR->incTotalScanCount(_scanIter->getTotalScanCount() - lastScanCount);
        if (_seekCount >= _limit) {
            eof = true;
            uint32_t delCount = _seekCount - _limit;
            if (delCount > 0) {
                uint32_t reserveCount = matchDocs.size() - delCount;
                matchDocAllocator->deallocate(matchDocs.data() + reserveCount, delCount);
                matchDocs.resize(reserveCount);
            }
        }
    } else {
        eof = true;
    }
    _scanInitParamR->incSeekTime(seekTimer.done_us());

    autil::ScopedTime2 evaluteTimer;
    evaluateAttribute(matchDocs, matchDocAllocator, _attributeExpressionVec, eof);
    _scanInitParamR->incEvaluateTime(evaluteTimer.done_us());

    autil::ScopedTime2 outputTimer;
    bool reuseMatchDocAllocator = _pushDownMode
                                  || (eof && _scanOnce && !_scanIteratorCreatorR->useMatchData())
                                  || (_scanInitParamR->sortDesc.topk != 0);
    table = createTable(matchDocs, matchDocAllocator, reuseMatchDocAllocator);
    _scanInitParamR->incOutputTime(outputTimer.done_us());

    if (eof && _scanIter) {
        _innerScanInfo.set_totalseekdoccount(_innerScanInfo.totalseekdoccount()
                                             + _scanIter->getTotalSeekDocCount());
        _innerScanInfo.set_usetruncate(_scanIter->useTruncate());
        _scanIter.reset();
    }
    getInvertedTracers(_tracerMap);
    collectInvertedInfos(_tracerMap);
    return true;
}

bool NormalScanR::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
    _scanOnce = false;
    _scanIter.reset();
    if (!inputQuery) {
        SQL_LOG(TRACE3, "input query is nullptr");
        return true;
    }
    if (!_scanIteratorCreatorR->updateQuery(inputQuery)) {
        return false;
    }
    bool emptyScan = false;
    _scanIter = _scanIteratorCreatorR->createScanIterator(emptyScan);
    auto scanIter = _scanIter.get();
    SQL_LOG(TRACE3,
            "after create scan iterator[%p] type[%s]",
            scanIter,
            scanIter ? typeid(*scanIter).name() : "");
    if (!_scanIter && !emptyScan) {
        SQL_LOG(
            WARN, "table [%s] create ha3 scan iterator failed", _scanInitParamR->tableName.c_str());
        return false;
    }
    return true;
}

bool NormalScanR::initOutputColumn() {
    const auto &outputFields = _scanInitParamR->calcInitParamR->outputFields;
    const auto &outputFieldsType = _scanInitParamR->calcInitParamR->outputFieldsType;
    _attributeExpressionVec.clear();
    _attributeExpressionVec.reserve(outputFields.size());
    std::map<std::string, ExprEntity> exprsMap;
    std::unordered_map<std::string, std::string> renameMap;
    const auto &outputExprsJson = _scanInitParamR->calcInitParamR->outputExprsJson;
    bool ret = ExprUtil::parseOutputExprs(
        _queryMemPoolR->getPool().get(), outputExprsJson, exprsMap, renameMap);
    if (!ret) {
        SQL_LOG(WARN, "parse output exprs [%s] failed", outputExprsJson.c_str());
        return false;
    }
    auto attributeInfos = _attributeExpressionCreatorR->_tableInfo->getAttributeInfos();
    std::map<std::string, std::pair<std::string, bool>> expr2Outputs;

    const auto &matchDocAllocator = _attributeExpressionCreatorR->_matchDocAllocator;
    const auto &attributeExpressionCreator
        = _attributeExpressionCreatorR->_attributeExpressionCreator;
    for (size_t i = 0; i < outputFields.size(); i++) {
        const auto &outputName = outputFields[i];
        std::string outputFieldType = "";
        if (outputFields.size() == outputFieldsType.size()) {
            outputFieldType = outputFieldsType[i];
        }
        std::string exprJson;
        std::string exprStr = outputName;
        suez::turing::AttributeExpression *expr = NULL;
        bool hasError = false;
        std::string errorInfo;
        auto iter = exprsMap.find(outputName);
        if (iter != exprsMap.end()) {
            exprJson = iter->second.exprJson;
            exprStr = iter->second.exprStr;
        }
        autil::AutilPoolAllocator allocator(_queryMemPoolR->getPool().get());
        autil::SimpleDocument simpleDoc(&allocator);
        if (!exprJson.empty() && ExprUtil::parseExprsJson(exprJson, simpleDoc)
            && ExprUtil::isCaseOp(simpleDoc)) {
            if (copyField(exprJson, outputName, expr2Outputs)) {
                continue;
            }
            expr = ExprUtil::CreateCaseExpression(attributeExpressionCreator,
                                                  outputFieldType,
                                                  simpleDoc,
                                                  hasError,
                                                  errorInfo,
                                                  _queryMemPoolR->getPool().get());
            if (hasError || expr == NULL) {
                SQL_LOG(WARN, "create case expression failed [%s]", errorInfo.c_str());
                return false;
            }
        } else {
            if (copyField(exprStr, outputName, expr2Outputs)) {
                continue;
            }
            expr = initAttributeExpr(
                _scanInitParamR->tableName, outputName, outputFieldType, exprStr, expr2Outputs);
            if (!expr) {
                if (exprStr == outputName
                    && _scanInitParamR->fieldInfos.find(outputName)
                           != _scanInitParamR->fieldInfos.end()
                    && attributeInfos->getAttributeInfo(outputName) == NULL) {
                    SQL_LOG(WARN,
                            "outputName [%s] not exist in attributes, use default null string expr",
                            outputName.c_str());
                    suez::turing::AtomicSyntaxExpr defaultSyntaxExpr(
                        std::string("null"), vt_string, suez::turing::STRING_VALUE);
                    expr = ExprUtil::createConstExpression(
                        _queryMemPoolR->getPool().get(), &defaultSyntaxExpr, vt_string);
                    if (expr) {
                        attributeExpressionCreator->addNeedDeleteExpr(expr);
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
        if (!expr->allocate(matchDocAllocator.get())) {
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
    matchDocAllocator->extend();
    matchDocAllocator->extendSub();
    return true;
}

bool NormalScanR::copyField(const std::string &expr,
                            const std::string &outputName,
                            std::map<std::string, std::pair<std::string, bool>> &expr2Outputs) {
    const auto &matchDocAllocator = _attributeExpressionCreatorR->_matchDocAllocator;
    auto iter = expr2Outputs.find(expr);
    if (iter != expr2Outputs.end()) {
        if (!iter->second.second) {
            _copyFieldMap[outputName] = iter->second.first;
            if (appendCopyColumn(iter->second.first, outputName, matchDocAllocator)) {
                return true;
            }
        }
    } else {
        expr2Outputs[expr] = make_pair(outputName, false);
    }
    return false;
}

std::pair<suez::turing::SyntaxExpr *, bool>
NormalScanR::parseSyntaxExpr(const std::string &tableName, const std::string &exprStr) {
    suez::turing::SyntaxExpr *syntaxExpr = nullptr;
    bool needDelete = false;
    if (!syntaxExpr) {
        syntaxExpr = suez::turing::SyntaxParser::parseSyntax(exprStr);
        needDelete = true;
    }
    return std::make_pair(syntaxExpr, needDelete);
}

suez::turing::AttributeExpression *
NormalScanR::initAttributeExpr(const std::string &tableName,
                               const std::string &outputName,
                               const std::string &outputFieldType,
                               const std::string &exprStr,
                               std::map<std::string, std::pair<std::string, bool>> &expr2Outputs) {
    const auto &attributeExpressionCreator
        = _attributeExpressionCreatorR->_attributeExpressionCreator;
    if (exprStr == isearch::common::INNER_DOCID_FIELD_NAME) {
        auto expr = POOL_NEW_CLASS(_queryMemPoolR->getPool().get(), InnerDocIdExpression);
        SQL_LOG(TRACE2, "create inner docid expression [%s]", exprStr.c_str());
        if (expr != nullptr) {
            attributeExpressionCreator->addNeedDeleteExpr(expr);
        }
        return expr;
    }

    auto exprResult = parseSyntaxExpr(tableName, exprStr);
    auto syntaxExpr = exprResult.first;

    auto needDelete = exprResult.second;
    if (!syntaxExpr) {
        SQL_LOG(WARN, "parse syntax [%s] failed", exprStr.c_str());
        return NULL;
    }

    suez::turing::AttributeExpression *expr = NULL;
    if (syntaxExpr->getSyntaxExprType() == suez::turing::SYNTAX_EXPR_TYPE_CONST_VALUE) {
        auto iter2 = expr2Outputs.find(exprStr);
        if (iter2 != expr2Outputs.end()) {
            iter2->second.second = true;
        }
        auto atomicExpr = dynamic_cast<suez::turing::AtomicSyntaxExpr *>(syntaxExpr);
        if (atomicExpr) {
            auto resultType = vt_unknown;
            auto attrInfos = _attributeExpressionCreatorR->_tableInfo->getAttributeInfos();
            if (attrInfos) {
                auto attrInfo = attrInfos->getAttributeInfo(outputName.c_str());
                if (attrInfo) {
                    auto fieldType = attrInfo->getFieldType();
                    resultType = suez::turing::TypeTransformer::transform(fieldType);
                }
            }
            if (resultType == vt_unknown) {
                if (outputFieldType != "") {
                    resultType = ExprUtil::transSqlTypeToVariableType(outputFieldType).first;
                } else {
                    auto type = atomicExpr->getAtomicSyntaxExprType();
                    if (type == suez::turing::FLOAT_VALUE) {
                        resultType = vt_float;
                    } else if (type == suez::turing::INTEGER_VALUE) {
                        resultType = vt_int32;
                    } else if (type == suez::turing::STRING_VALUE) {
                        resultType = vt_string;
                    }
                }
            }
            expr = ExprUtil::createConstExpression(
                _queryMemPoolR->getPool().get(), atomicExpr, resultType);
            if (expr) {
                SQL_LOG(TRACE2,
                        "create const expression [%s], result type [%d]",
                        exprStr.c_str(),
                        resultType)
                attributeExpressionCreator->addNeedDeleteExpr(expr);
            }
        }
    }
    if (!expr) {
        expr = attributeExpressionCreator->createAttributeExpression(syntaxExpr, true);
    }
    if (needDelete) {
        DELETE_AND_SET_NULL(syntaxExpr);
    }
    return expr;
}

bool NormalScanR::appendCopyColumn(const std::string &srcField,
                                   const std::string &destField,
                                   const matchdoc::MatchDocAllocatorPtr &matchDocAllocator) {
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

void NormalScanR::copyColumns(const std::map<std::string, std::string> &copyFieldMap,
                              const std::vector<matchdoc::MatchDoc> &matchDocVec,
                              const matchdoc::MatchDocAllocatorPtr &matchDocAllocator) {
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
                std::function<void(matchdoc::MatchDoc)> cloneSub
                    = [srcRef, destRef](matchdoc::MatchDoc newDoc) {
                          destRef->cloneConstruct(newDoc, newDoc, srcRef);
                      };
                accessor->foreach (matchDoc, cloneSub);
            } else {
                destRef->cloneConstruct(matchDoc, matchDoc, srcRef);
            }
        }
    }
}

void NormalScanR::evaluateAttribute(
    std::vector<matchdoc::MatchDoc> &matchDocVec,
    const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
    std::vector<suez::turing::AttributeExpression *> &attributeExpressionVec,
    bool eof) {
    suez::turing::ExpressionEvaluator<std::vector<suez::turing::AttributeExpression *>> evaluator(
        attributeExpressionVec, matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluateExpressions(matchDocVec.data(), matchDocVec.size());
    copyColumns(_copyFieldMap, matchDocVec, matchDocAllocator);
}

matchdoc::MatchDocAllocatorPtr
NormalScanR::copyMatchDocAllocator(std::vector<matchdoc::MatchDoc> &matchDocVec,
                                   const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                                   bool reuseMatchDocAllocator,
                                   std::vector<matchdoc::MatchDoc> &copyMatchDocs) {
    auto outputAllocator = matchDocAllocator;
    if (!reuseMatchDocAllocator) {
        outputAllocator.reset(matchDocAllocator->cloneAllocatorWithoutData());
        if (!matchDocAllocator->swapDocStorage(*outputAllocator, copyMatchDocs, matchDocVec)) {
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

table::TablePtr NormalScanR::doCreateTable(matchdoc::MatchDocAllocatorPtr outputAllocator,
                                           std::vector<matchdoc::MatchDoc> copyMatchDocs) {
    if (_useSubR->getUseSub()) {
        table::TablePtr table;
        flattenSub(outputAllocator, copyMatchDocs, table);
        auto calcTableR = CalcTableR::buildOne(_ctx,
                                               _scanInitParamR->calcInitParamR->outputFields,
                                               _scanInitParamR->calcInitParamR->outputFieldsType,
                                               "",
                                               "");
        if (!calcTableR) {
            SQL_LOG(WARN, "build CalcTableR failed");
            return table;
        }
        if (!calcTableR->projectTable(table)) {
            SQL_LOG(
                WARN, "project table [%s] failed.", table::TableUtil::toString(table, 5).c_str());
        }
        return table;
    } else {
        table::TablePtr table(new table::Table(copyMatchDocs, outputAllocator));
        if (_scanInitParamR->sortDesc.topk != 0) {
            auto calcTableR
                = CalcTableR::buildOne(_ctx,
                                       _scanInitParamR->calcInitParamR->outputFields,
                                       _scanInitParamR->calcInitParamR->outputFieldsType,
                                       "",
                                       "");
            if (!calcTableR) {
                SQL_LOG(WARN, "build CalcTableR failed");
                return table;
            }
            if (!calcTableR->projectTable(table)) {
                SQL_LOG(WARN,
                        "sort project table [%s] failed.",
                        table::TableUtil::toString(table, 5).c_str());
            }
        }
        return table;
    }
}

void NormalScanR::flattenSub(matchdoc::MatchDocAllocatorPtr &outputAllocator,
                             const std::vector<matchdoc::MatchDoc> &copyMatchDocs,
                             table::TablePtr &table) {
    std::vector<matchdoc::MatchDoc> outputMatchDocs;
    auto accessor = outputAllocator->getSubDocAccessor();
    std::function<void(matchdoc::MatchDoc)> processNothing
        = [&outputMatchDocs](matchdoc::MatchDoc newDoc) { outputMatchDocs.push_back(newDoc); };
    auto firstSubRef = outputAllocator->getFirstSubDocRef();
    std::vector<matchdoc::MatchDoc> needConstructSubDocs;
    for (auto doc : copyMatchDocs) {
        auto &first = firstSubRef->getReference(doc);
        if (first != matchdoc::INVALID_MATCHDOC) {
            accessor->foreachFlatten(doc, outputAllocator.get(), processNothing);
        } else {
            if (_nestTableJoinType == INNER_JOIN) {
                continue;
            }
            // alloc empty sub doc
            auto subDoc = outputAllocator->allocateSub(doc);
            needConstructSubDocs.emplace_back(subDoc);
            outputMatchDocs.push_back(doc);
        }
    }
    if (!needConstructSubDocs.empty()) {
        auto subRefs = outputAllocator->getAllSubNeedSerializeReferences(0);
        for (auto &ref : subRefs) {
            for (auto subDoc : needConstructSubDocs) {
                ref->construct(subDoc);
            }
        }
    }
    table.reset(new table::Table(outputMatchDocs, outputAllocator));
}

void NormalScanR::getInvertedTracers(
    std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap) {
    auto invertedTracers
        = _attributeExpressionCreatorR->_indexPartitionReaderWrapper->getInvertedTracers();
    tracerMap.clear();
    auto *indexInfos = _attributeExpressionCreatorR->_tableInfo->getIndexInfos();
    for (const auto &pair : invertedTracers) {
        auto &indexName = *pair.first;
        auto *indexInfo = indexInfos->getIndexInfo(indexName.c_str());
        assert(indexInfo && "index info must exist");
        auto indexType = indexInfo->getIndexType();
        auto indexTypeStr = ContainToQueryImpl::indexTypeToString(indexType);
        auto *tracer = pair.second;
        SQL_LOG(TRACE3,
                "index[%s] with inverted tracer[%s]",
                indexName.c_str(),
                autil::StringUtil::toString(*tracer).c_str());
        auto &mergeTracer = tracerMap[indexTypeStr];
        mergeTracer += *tracer;
    }
}

void NormalScanR::collectInvertedInfos(
    const std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap) {
    if (!_sqlSearchInfoCollectorR) {
        return;
    }
    const auto &collector = _sqlSearchInfoCollectorR->getCollector();
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

void NormalScanR::collectBlockInfo(const indexlib::util::BlockAccessCounter *counter,
                                   BlockAccessInfo &blockInfo) {
    blockInfo.set_blockcachehitcount(counter->blockCacheHitCount);
    blockInfo.set_blockcachemisscount(counter->blockCacheMissCount);
    blockInfo.set_blockcachereadlatency(counter->blockCacheReadLatency);
    blockInfo.set_blockcacheiocount(counter->blockCacheIOCount);
    blockInfo.set_blockcacheiodatasize(counter->blockCacheIODataSize);
}

void NormalScanR::reportInvertedMetrics(
    std::map<std::string, indexlib::index::InvertedIndexSearchTracer> &tracerMap) {
    if (!_queryMetricReporterR) {
        return;
    }
    const auto &metricsReporter = _queryMetricReporterR->getReporter();
    for (auto &pair : tracerMap) {
        auto &indexTypeStr = pair.first;
        auto *tracer = &pair.second;
        auto reporter = metricsReporter->getSubReporter(
            "", {{{"table_name", getTableNameForMetrics()}, {"index_type", indexTypeStr}}});
        reporter->report<InvertedIndexMetrics, indexlib::index::InvertedIndexSearchTracer>(nullptr,
                                                                                           tracer);
    }
}

REGISTER_RESOURCE(NormalScanR);

} // namespace sql
