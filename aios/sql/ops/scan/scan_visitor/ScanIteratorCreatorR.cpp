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
#include "sql/ops/scan/ScanIteratorCreatorR.h"

#include <assert.h>
#include <exception>
#include <ext/alloc_traits.h>
#include <limits>
#include <map>
#include <set>
#include <stddef.h>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/DocIdsQuery.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/QueryFlatten.h"
#include "ha3/common/QueryTermVisitor.h"
#include "ha3/common/TermCombineVisitor.h"
#include "ha3/isearch.h"
#include "ha3/search/AuxiliaryChainVisitor.h"
#include "ha3/search/DefaultLayerMetaUtil.h"
#include "ha3/search/Filter.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/JoinFilter.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/QueryExecutorCreator.h"
#include "ha3/search/RangeQueryExecutor.h"
#include "ha3/search/SubMatchCheckVisitor.h"
#include "ha3/search/SubMatchVisitor.h"
#include "ha3/search/TermMatchVisitor.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/JsonMap.h"
#include "matchdoc/MatchDocAllocator.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/scan/DocIdRangesReduceOptimize.h"
#include "sql/ops/scan/DocIdsScanIterator.h"
#include "sql/ops/scan/Ha3ScanConditionVisitor.h"
#include "sql/ops/scan/Ha3ScanConditionVisitorParam.h"
#include "sql/ops/scan/Ha3ScanIterator.h"
#include "sql/ops/scan/OrderedHa3ScanIterator.h"
#include "sql/ops/scan/QueryExecutorExpressionWrapper.h"
#include "sql/ops/scan/QueryExprFilterWrapper.h"
#include "sql/ops/scan/QueryScanIterator.h"
#include "sql/ops/scan/RangeScanIterator.h"
#include "sql/ops/scan/RangeScanIteratorWithoutFilter.h"
#include "sql/ops/scan/udf_to_query/UdfToQueryManager.h"
#include "sql/ops/sort/SortInitParam.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/JoinDocIdConverterCreator.h"
#include "suez/turing/expression/provider/MetaInfo.h"
#include "suez/turing/expression/provider/common.h"
#include "suez/turing/expression/util/IndexInfoHelper.h"
#include "suez/turing/expression/util/LegacyIndexInfoHelper.h"

namespace indexlib {
namespace index {
class DeletionMapReaderAdaptor;
class JoinDocidAttributeIterator;
} // namespace index
} // namespace indexlib
namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string ScanIteratorCreatorR::RESOURCE_ID = "scan_iterator_creator_r";

ScanIteratorCreatorR::ScanIteratorCreatorR() {}

ScanIteratorCreatorR::~ScanIteratorCreatorR() {
    if (_indexInfoHelper) {
        POOL_DELETE_CLASS(_indexInfoHelper);
    }
}

void ScanIteratorCreatorR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool ScanIteratorCreatorR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode ScanIteratorCreatorR::init(navi::ResourceInitContext &ctx) {
    _indexPartitionReaderWrapper = _attributeExpressionCreatorR->_indexPartitionReaderWrapper;
    _tableInfo = _attributeExpressionCreatorR->_tableInfo;
    _matchDocAllocator = _attributeExpressionCreatorR->_matchDocAllocator;
    _attributeExpressionCreator = _attributeExpressionCreatorR->_attributeExpressionCreator;
    _functionProvider = _attributeExpressionCreatorR->_functionProvider;
    if (!initMatchData()) {
        return navi::EC_ABORT;
    }
    const auto &tableName = _scanInitParamR->tableName;
    auto pool = _queryMemPoolR->getPool().get();
    ConditionParser parser(pool);
    ConditionPtr condition;
    const auto &conditionInfo = _scanInitParamR->calcInitParamR->conditionJson;
    if (!parser.parseCondition(conditionInfo, condition)) {
        SQL_LOG(ERROR,
                "table name [%s],  parse condition [%s] failed",
                tableName.c_str(),
                conditionInfo.c_str());
        return navi::EC_ABORT;
    }
    isearch::search::LayerMetaPtr layerMeta = createLayerMeta();
    if (!layerMeta) {
        SQL_LOG(WARN, "table name [%s], create layer meta failed.", tableName.c_str());
        return navi::EC_ABORT;
    }
    SQL_LOG(DEBUG, "layer meta: %s", layerMeta->toString().c_str());
    const auto &tableSortDescription = _ha3TableInfoR->getTableSortDescMap();
    auto iter = tableSortDescription.find(tableName);
    if (iter != tableSortDescription.end()) {
        SQL_LOG(DEBUG, "begin reduce docid range optimize");
        DocIdRangesReduceOptimize optimize(iter->second, _scanInitParamR->fieldInfos);
        if (condition) {
            condition->accept(&optimize);
        }
        layerMeta = optimize.reduceDocIdRange(layerMeta, pool, _indexPartitionReaderWrapper);
        SQL_LOG(DEBUG,
                "after reduce docid range optimize, layer meta: %s",
                layerMeta->toString().c_str());
    } else {
        SQL_LOG(DEBUG, "not find table [%s] sort description", tableName.c_str());
    }
    if (layerMeta) {
        layerMeta->quotaMode = QM_PER_DOC;
        proportionalLayerQuota(*layerMeta.get());
        SQL_LOG(
            DEBUG, "after proportional layer quota, layer meta: %s", layerMeta->toString().c_str());
    }

    Ha3ScanConditionVisitorParam param;
    param.attrExprCreator = _attributeExpressionCreator;
    param.indexPartitionReaderWrapper = _indexPartitionReaderWrapper;
    param.analyzerFactory = _analyzerFactoryR->getFactory().get();
    param.indexInfo = &_scanInitParamR->indexInfos;
    param.pool = pool;
    param.queryInfo = &_ha3QueryInfoR->getQueryInfo();
    param.indexInfos = _tableInfo->getIndexInfos();
    param.mainTableName = tableName;
    param.timeoutTerminator = _timeoutTerminatorR->getTimeoutTerminator();
    param.layerMeta = layerMeta.get();
    param.modelConfigMap = &_modelConfigMapR->getModelConfigMap();
    param.udfToQueryManager = _udfToQueryManagerR->getManager().get();
    param.forbidIndexs = &_scanInitParamR->forbidIndexs;
    Ha3ScanConditionVisitor visitor(param);
    if (condition) {
        condition->accept(&visitor);
    }
    if (visitor.isError()) {
        SQL_LOG(WARN,
                "table name [%s], create scan iterator failed, error info [%s]",
                tableName.c_str(),
                visitor.errorInfo().c_str());
        return navi::EC_ABORT;
    }
    isearch::common::QueryPtr query(visitor.stealQuery());
    if (query) {
        SQL_LOG(TRACE1, "query [%s]", query->toString().c_str());
        isearch::common::QueryFlatten queryFlatten;
        queryFlatten.flatten(query.get());
        isearch::common::Query *flattenQuery = queryFlatten.stealQuery();
        assert(flattenQuery);
        query.reset(flattenQuery);
        SQL_LOG(TRACE1, "after flat query [%s]", query->toString().c_str());

        auto tabletSchema = _indexPartitionReaderWrapper->getSchema();
        if (!tabletSchema) {
            SQL_LOG(ERROR, "get index schema failed from table [%s]", tableName.c_str());
            return navi::EC_ABORT;
        }

        termCombine(query, tabletSchema);

        if (!initTruncateDesc(query)) {
            return navi::EC_ABORT;
        }
        if (!_scanInitParamR->matchType.empty()) {
            SQL_LOG(TRACE1, "before sub match query");
            auto matchDataLabel = getMatchDataLabel();
            subMatchQuery(query, tabletSchema, matchDataLabel);
            SQL_LOG(TRACE1, "after sub match query [%s]", query->toString().c_str());
        }
    }
    suez::turing::AttributeExpression *attrExpr = visitor.stealAttributeExpression();
    isearch::search::FilterWrapperPtr filterWrapper;
    if (attrExpr || !_scanInitParamR->auxTableName.empty()) {
        SQL_LOG(
            TRACE1, "condition expr [%s]", attrExpr ? attrExpr->getOriginalString().c_str() : "");
        if (!createJoinDocIdConverter()) {
            return navi::EC_ABORT;
        }
        bool ret = createFilterWrapper(attrExpr,
                                       visitor.getQueryExprs(),
                                       _attributeExpressionCreator->getJoinDocIdConverterCreator(),
                                       filterWrapper);
        if (!ret) {
            SQL_LOG(WARN,
                    "table name [%s], create filter wrapper failed, exprStr: [%s]",
                    _scanInitParamR->tableName.c_str(),
                    attrExpr->getOriginalString().c_str());
            return navi::EC_ABORT;
        }
    }
    _createScanIteratorInfo.query = query;
    _createScanIteratorInfo.filterWrapper = filterWrapper;
    _createScanIteratorInfo.queryExprs = visitor.getQueryExprs();
    _createScanIteratorInfo.layerMeta = layerMeta;
    _baseScanIteratorInfo = _createScanIteratorInfo;
    return navi::EC_NONE;
}

bool ScanIteratorCreatorR::initMatchData() {
    _matchDataManager = std::make_shared<isearch::search::MatchDataManager>();
    const auto &matchType = _scanInitParamR->matchType;
    if (matchType.empty()) {
        return true;
    }
    SQL_LOG(TRACE1,
            "require match data, match types [%s]",
            autil::StringUtil::toString(matchType).c_str());
    _matchDataManager->requireMatchData();
    _matchDataManager->setQueryCount(1);
    return true;
}

bool ScanIteratorCreatorR::initTruncateDesc(isearch::common::QueryPtr &query) {
    auto scanHintMap = _scanInitParamR->getScanHintMap();
    if (!scanHintMap) {
        return true;
    }
    const auto &hints = *scanHintMap;
    auto iter = hints.find("truncateDesc");
    if (iter == hints.end()) {
        return true;
    }
    const auto &truncateDesc = iter->second;
    if (!truncateDesc.empty()) {
        SQL_LOG(TRACE1, "use truncate query");
        try {
            truncateQuery(query, truncateDesc);
        } catch (const autil::legacy::ExceptionBase &e) {
            SQL_LOG(ERROR, "scanInitParam init failed error:[%s].", e.what());
            return false;
        } catch (...) { return false; }
        SQL_LOG(TRACE1, "after truncate query [%s]", query->toString().c_str());
    }
    return true;
}

std::string ScanIteratorCreatorR::getMatchDataLabel() const {
    std::string ret = "sub";
    auto scanHintMap = _scanInitParamR->getScanHintMap();
    if (!scanHintMap) {
        return ret;
    }
    const auto &hints = *scanHintMap;
    auto iter = hints.find("MatchDataLabel");
    if (iter != hints.end()) {
        if (iter->second == "full_term") {
            ret = "full_term";
        }
    }
    return ret;
}

void ScanIteratorCreatorR::requireMatchData() {
    auto pool = _queryMemPoolR->getPool().get();
    std::unordered_set<std::string> matchTypeSet(_scanInitParamR->matchType.begin(),
                                                 _scanInitParamR->matchType.end());

    if (matchTypeSet.count(SQL_MATCH_TYPE_SUB) != 0) {
        if (_matchDocAllocator->hasSubDocAllocator()) {
            SQL_LOG(DEBUG, "require sub simple match data");
            _matchDataManager->requireSubSimpleMatchData(
                _matchDocAllocator.get(), SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_GROUP, pool);
        } else {
            SQL_LOG(DEBUG, "require simple match data");
            _matchDataManager->requireSimpleMatchData(
                _matchDocAllocator.get(), SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, pool);
        }
    } else if (matchTypeSet.count(SQL_MATCH_TYPE_SIMPLE) != 0) {
        SQL_LOG(DEBUG, "require simple match data");
        _matchDataManager->requireSimpleMatchData(
            _matchDocAllocator.get(), SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, pool);
    }
    if (matchTypeSet.count(SQL_MATCH_TYPE_FULL) != 0) {
        SQL_LOG(DEBUG, "require full match data");
        _matchDataManager->requireMatchData(
            _matchDocAllocator.get(), MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, pool);
    }
    if (matchTypeSet.count(SQL_MATCH_TYPE_VALUE) != 0) {
        SQL_LOG(DEBUG, "require match value");
        _matchDataManager->requireMatchValues(
            _matchDocAllocator.get(), MATCH_VALUE_REF, SUB_DOC_DISPLAY_NO, pool);
    }
}

void ScanIteratorCreatorR::postInitMatchData() {
    if (_scanInitParamR->matchType.empty()) {
        return;
    }
    if (!_metaInfo) {
        _metaInfo = std::make_shared<suez::turing::MetaInfo>();
        _matchDataManager->getQueryTermMetaInfo(_metaInfo.get());
        _indexInfoHelper = POOL_NEW_CLASS(_queryMemPoolR->getPool().get(),
                                          suez::turing::LegacyIndexInfoHelper,
                                          _tableInfo->getIndexInfos());
    }
    requireMatchData();
    if (_functionProvider->getMatchInfoReader() == nullptr) {
        auto metaInfo = std::make_shared<suez::turing::MetaInfo>();
        _matchDataManager->getQueryTermMetaInfo(metaInfo.get());
        _functionProvider->initMatchInfoReader(std::move(metaInfo));
        _functionProvider->setIndexInfoHelper(_indexInfoHelper);
        _functionProvider->setIndexReaderPtr(_indexPartitionReaderWrapper->getIndexReader());
    }
}

bool ScanIteratorCreatorR::updateQuery(const StreamQueryPtr &inputQuery) {
    const auto &query = inputQuery->query;
    if (!query) {
        return true;
    }
    SQL_LOG(TRACE2, "update query [%s]", query->toString().c_str());
    if (_baseScanIteratorInfo.query != nullptr) {
        isearch::common::QueryPtr andQuery(new isearch::common::AndQuery(""));
        andQuery->addQuery(query);
        andQuery->addQuery(_baseScanIteratorInfo.query);
        _createScanIteratorInfo.query = andQuery;
    } else {
        _createScanIteratorInfo.query = query;
    }
    SQL_LOG(TRACE2, "updated query [%s]", _createScanIteratorInfo.query->toString().c_str());
    if (_updateQueryCount != 0 && _createScanIteratorInfo.filterWrapper != nullptr) {
        _createScanIteratorInfo.filterWrapper->resetFilter();
    }
    _updateQueryCount++;
    if (!_scanInitParamR->matchType.empty()) {
        _matchDataManager->requireMatchData();
        _matchDataManager->setQueryCount(1);
    }
    return true;
}

bool ScanIteratorCreatorR::createJoinDocIdConverter() {
    const auto &auxTableName = _scanInitParamR->auxTableName;
    if (!auxTableName.empty()) {
        if (!_attributeExpressionCreator->createJoinDocIdConverter(auxTableName,
                                                                   _scanInitParamR->tableName)) {
            SQL_LOG(WARN,
                    "aux join create join doc id converter failed, aux [%s], main [%s]",
                    auxTableName.c_str(),
                    _scanInitParamR->tableName.c_str());
            return false;
        }
    }
    return true;
}

void ScanIteratorCreatorR::truncateQuery(isearch::common::QueryPtr &query,
                                         const std::string &truncateDesc) {
    std::vector<std::string> truncateNames;
    std::vector<isearch::search::SelectAuxChainType> types;
    if (!parseTruncateDesc(truncateDesc, truncateNames, types)) {
        return;
    }
    for (size_t i = 0; i < truncateNames.size(); ++i) {
        isearch::search::TermDFMap termDFMap;
        if (types[i] != isearch::search::SAC_ALL) {
            isearch::common::QueryTermVisitor visitor(
                isearch::common::QueryTermVisitor::VT_ANDNOT_QUERY);
            query->accept(&visitor);
            const auto &termVector = visitor.getTermVector();
            for (const auto &term : termVector) {
                termDFMap[term] = _indexPartitionReaderWrapper->getTermDF(term);
            }
        }
        isearch::search::AuxiliaryChainVisitor visitor(truncateNames[i], termDFMap, types[i]);
        query->accept(&visitor);
    }
}

bool ScanIteratorCreatorR::parseTruncateDesc(
    const std::string &truncateDesc,
    std::vector<std::string> &truncateNames,
    std::vector<isearch::search::SelectAuxChainType> &types) {
    for (const auto &auxChain : autil::StringUtil::split(truncateDesc, ";")) {
        auto type = isearch::search::SelectAuxChainType::SAC_DF_SMALLER;
        std::string truncateName;
        for (const auto &option : autil::StringUtil::split(auxChain, "|")) {
            const std::vector<std::string> &st = autil::StringUtil::split(option, "#");
            if (st.size() != 2) {
                SQL_LOG(WARN, "Invalid truncate query option[%s], skip optimize", option.c_str());
                return false;
            }
            if (st[0] == "select") {
                if (st[1] == "bigger" || st[1] == "BIGGER") {
                    type = isearch::search::SelectAuxChainType::SAC_DF_BIGGER;
                } else if (st[1] == "ALL" || st[1] == "all") {
                    type = isearch::search::SelectAuxChainType::SAC_ALL;
                } else if (st[1] == "smaller" || st[1] == "SMALLER") {
                    type = isearch::search::SelectAuxChainType::SAC_DF_SMALLER;
                } else {
                    SQL_LOG(WARN,
                            "Invalid truncate query select type [%s], skip optimize",
                            st[1].c_str());
                    return false;
                }
            } else if (st[0] == "aux_name") {
                truncateName = st[1];
            }
        }
        if (truncateName.empty()) {
            SQL_LOG(WARN, "Invalid truncate query, truncate name not found");
            return false;
        }
        truncateNames.push_back(truncateName);
        types.push_back(type);
    }
    return true;
}

void ScanIteratorCreatorR::subMatchQuery(
    isearch::common::QueryPtr &query,
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &indexSchemaPtr,
    const std::string &matchDataLabel) {
    isearch::search::SubMatchCheckVisitor subMatchCheckVisitor;
    query->accept(&subMatchCheckVisitor);
    if (subMatchCheckVisitor.needSubMatch()) {
        isearch::search::SubMatchVisitor subMatchVisitor;
        query->accept(&subMatchVisitor);
        if (matchDataLabel == "full_term") {
            isearch::search::TermMatchVisitor termMatchVisitor(indexSchemaPtr);
            query->accept(&termMatchVisitor);
        }
    }
}

ScanIteratorPtr ScanIteratorCreatorR::createScanIterator(bool &emptyScan) {
    const auto &query = _createScanIteratorInfo.query;
    const auto &filterWrapper = _createScanIteratorInfo.filterWrapper;
    const auto &queryExprs = _createScanIteratorInfo.queryExprs;
    const auto &layerMeta = _createScanIteratorInfo.layerMeta;
    emptyScan = false;
    if (!layerMeta) {
        SQL_LOG(WARN, "table name [%s], layer meta is null.", _scanInitParamR->tableName.c_str());
        return NULL;
    }
    if (layerMeta->size() == 0) {
        SQL_LOG(TRACE1, "table name [%s], empty layer meta.", _scanInitParamR->tableName.c_str());
        emptyScan = true;
        return NULL;
    }
    std::vector<isearch::search::LayerMetaPtr> layerMetas;
    ScanIteratorPtr scanIterator = ScanIteratorPtr();
    if (_scanInitParamR->sortDesc.topk != 0) { // has sort desc
        SQL_LOG(TRACE2, "create ordered ha3 scan iter");
        for (size_t i = 0; i < layerMeta->size(); ++i) {
            isearch::search::LayerMetaPtr singleLayerMeta(
                new isearch::search::LayerMeta(*layerMeta));
            singleLayerMeta->clear();
            singleLayerMeta->push_back((*layerMeta)[i]);
            layerMetas.emplace_back(singleLayerMeta);
        }
        scanIterator.reset(
            createOrderedHa3ScanIterator(query, filterWrapper, layerMetas, emptyScan));
    } else if (dynamic_cast<isearch::common::DocIdsQuery *>(query.get()) != nullptr
               && filterWrapper == nullptr) {
        SQL_LOG(TRACE2, "create docids scan iter");
        scanIterator.reset(createDocIdScanIterator(query));
    } else if (needHa3Scan(query)) {
        SQL_LOG(TRACE2, "create ha3 scan iter");
        scanIterator.reset(createHa3ScanIterator(query, filterWrapper, layerMeta, emptyScan));
        layerMetas = {layerMeta};
    } else if (nullptr == query) {
        SQL_LOG(TRACE2, "create range scan iter");
        scanIterator.reset(createRangeScanIterator(filterWrapper, layerMeta));
        layerMetas = {layerMeta};
    } else {
        SQL_LOG(TRACE2, "create query scan iter");
        scanIterator.reset(createQueryScanIterator(query, filterWrapper, layerMeta, emptyScan));
        layerMetas = {layerMeta};
    }
    for (auto *expr : queryExprs) {
        auto *queryExpr = dynamic_cast<QueryExecutorExpressionWrapper *>(expr);
        if (queryExpr == nullptr) {
            SQL_LOG(ERROR,
                    "unexpected query expression cast failed, [%s]",
                    expr->getOriginalString().c_str());
            return ScanIteratorPtr();
        }
        if (!queryExpr->init(_indexPartitionReaderWrapper,
                             _scanInitParamR->tableName,
                             _queryMemPoolR->getPool().get(),
                             _timeoutTerminatorR->getTimeoutTerminator(),
                             layerMetas)) {
            SQL_LOG(ERROR,
                    "unexpected init query expression failed, [%s]",
                    expr->getOriginalString().c_str());
            return ScanIteratorPtr();
        }
    }
    postInitMatchData();
    return scanIterator;
}

bool ScanIteratorCreatorR::needHa3Scan(const isearch::common::QueryPtr &query) {
    return _matchDocAllocator->hasSubDocAllocator() || (_matchDataManager->needMatchData())
           || (query != nullptr && _scanInitParamR->limit != std::numeric_limits<uint32_t>::max());
}

bool ScanIteratorCreatorR::isTermQuery(const isearch::common::QueryPtr &query) {
    if (query == NULL) {
        return false;
    }
    auto type = query->getType();
    if (type == TERM_QUERY || type == NUMBER_QUERY) {
        return true;
    }
    return false;
}

ScanIterator *ScanIteratorCreatorR::createRangeScanIterator(
    const isearch::search::FilterWrapperPtr &filterWrapper,
    const isearch::search::LayerMetaPtr &layerMeta) {
    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    delMapReader = _indexPartitionReaderWrapper->getDeletionMapReader();
    if (filterWrapper != NULL) {
        return new RangeScanIterator(filterWrapper,
                                     _matchDocAllocator,
                                     delMapReader,
                                     layerMeta,
                                     _timeoutTerminatorR->getTimeoutTerminator());
    } else {
        return new RangeScanIteratorWithoutFilter(_matchDocAllocator,
                                                  delMapReader,
                                                  layerMeta,
                                                  _timeoutTerminatorR->getTimeoutTerminator());
    }
}

ScanIterator *ScanIteratorCreatorR::createQueryScanIterator(
    const isearch::common::QueryPtr &query,
    const isearch::search::FilterWrapperPtr &filterWrapper,
    const isearch::search::LayerMetaPtr &layerMeta,
    bool &emptyScan) {
    auto queryExecutor
        = createQueryExecutor(query, _scanInitParamR->tableName, layerMeta.get(), emptyScan);
    if (emptyScan) {
        SQL_LOG(TRACE1, "empty result scan condition. query [%s]", query->toString().c_str());
        return NULL;
    }
    if (!queryExecutor) {
        SQL_LOG(WARN,
                "table name [%s], create query executor failed. query [%s]",
                _scanInitParamR->tableName.c_str(),
                query->toString().c_str());
        return NULL;
    }
    isearch::search::QueryExecutorPtr queryExecutorPtr(
        queryExecutor, [](isearch::search::QueryExecutor *p) { POOL_DELETE_CLASS(p); });
    if (!_useSubR->getUseSub() && queryExecutor->hasSubDocExecutor()) {
        SQL_LOG(ERROR,
                "use sub query [%s] without unnest sub table",
                queryExecutor->toString().c_str());
        return NULL;
    }

    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    delMapReader = _indexPartitionReaderWrapper->getDeletionMapReader();
    return new QueryScanIterator(queryExecutorPtr,
                                 filterWrapper,
                                 _matchDocAllocator,
                                 delMapReader,
                                 layerMeta,
                                 _timeoutTerminatorR->getTimeoutTerminator());
}

ScanIterator *
ScanIteratorCreatorR::createHa3ScanIterator(const isearch::common::QueryPtr &query,
                                            const isearch::search::FilterWrapperPtr &filterWrapper,
                                            const isearch::search::LayerMetaPtr &layerMeta,
                                            bool &emptyScan) {
    std::vector<isearch::search::QueryExecutorPtr> queryExecutors;
    if (!createQueryExecutors(
            query, _scanInitParamR->tableName, {layerMeta}, emptyScan, queryExecutors)) {
        SQL_LOG(TRACE1, "create query executors failed");
        return nullptr;
    }

    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    indexlib::index::DeletionMapReaderPtr subDelMapReader;
    indexlib::index::JoinDocidAttributeIterator *mainToSubIt = NULL;
    delMapReader = _indexPartitionReaderWrapper->getDeletionMapReader();

    if (_matchDocAllocator->hasSubDocAllocator()) {
        mainToSubIt = _indexPartitionReaderWrapper->getMainToSubIter();
        const indexlib::partition::IndexPartitionReaderPtr &subIndexPartReader
            = _indexPartitionReaderWrapper->getSubReader();
        if (subIndexPartReader) {
            subDelMapReader = subIndexPartReader->GetDeletionMapReader();
        }
    }

    Ha3ScanIteratorParam seekParam;
    seekParam.queryExecutors = queryExecutors;
    seekParam.filterWrapper = filterWrapper;
    seekParam.matchDocAllocator = _matchDocAllocator;
    seekParam.delMapReader = delMapReader;
    seekParam.subDelMapReader = subDelMapReader;
    seekParam.layerMetas = {layerMeta};
    if (query != nullptr) {
        seekParam.matchDataManager = _matchDataManager;
    }
    seekParam.mainToSubIt = mainToSubIt;
    seekParam.timeoutTerminator = _timeoutTerminatorR->getTimeoutTerminator();
    seekParam.needAllSubDocFlag = false;
    return new Ha3ScanIterator(seekParam);
}

ScanIterator *ScanIteratorCreatorR::createOrderedHa3ScanIterator(
    const isearch::common::QueryPtr &query,
    const isearch::search::FilterWrapperPtr &filterWrapper,
    const std::vector<isearch::search::LayerMetaPtr> &layerMetas,
    bool &emptyScan) {
    if (_matchDocAllocator->hasSubDocAllocator() || _useSubR->getUseSub()) {
        SQL_LOG(ERROR, "unexpected ordered scan iter has UNNEST table");
        return nullptr;
    }

    std::vector<isearch::search::QueryExecutorPtr> queryExecutors;
    if (!createQueryExecutors(
            query, _scanInitParamR->tableName, layerMetas, emptyScan, queryExecutors)) {
        SQL_LOG(TRACE1, "create query executors failed");
        return nullptr;
    }

    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    delMapReader = _indexPartitionReaderWrapper->getDeletionMapReader();

    Ha3ScanIteratorParam seekParam;
    seekParam.queryExecutors = queryExecutors;
    seekParam.filterWrapper = filterWrapper;
    seekParam.matchDocAllocator = _matchDocAllocator;
    seekParam.delMapReader = delMapReader;
    seekParam.subDelMapReader = nullptr;
    seekParam.layerMetas = layerMetas;
    if (query != nullptr) {
        seekParam.matchDataManager = _matchDataManager;
    }
    seekParam.mainToSubIt = nullptr;
    seekParam.timeoutTerminator = _timeoutTerminatorR->getTimeoutTerminator();
    seekParam.needAllSubDocFlag = false;
    OrderedHa3ScanIterator *orderedHa3ScanIterator(
        new OrderedHa3ScanIterator(seekParam,
                                   _queryMemPoolR->getPool().get(),
                                   _scanInitParamR->sortDesc,
                                   _attributeExpressionCreator.get()));
    if (!orderedHa3ScanIterator->init()) {
        SQL_LOG(WARN, "create orderedHa3ScanIterator failed");
        delete orderedHa3ScanIterator;
        return nullptr;
    }
    return orderedHa3ScanIterator;
}

bool ScanIteratorCreatorR::createQueryExecutors(
    const isearch::common::QueryPtr &query,
    const std::string &mainTableName,
    const std::vector<isearch::search::LayerMetaPtr> &layerMetas,
    bool &emptyScan,
    std::vector<isearch::search::QueryExecutorPtr> &queryExecutors) {
    for (size_t i = 0; i < layerMetas.size(); ++i) {
        auto queryExecutor
            = createQueryExecutor(query, mainTableName, layerMetas[i].get(), emptyScan);
        if (emptyScan) {
            SQL_LOG(TRACE1,
                    "table name [%s], empty result scan condition. query [%s]",
                    mainTableName.c_str(),
                    query->toString().c_str());
            return false;
        }
        if (!queryExecutor) {
            SQL_LOG(WARN,
                    "table name [%s], create query executor failed. query [%s]",
                    mainTableName.c_str(),
                    query->toString().c_str());
            return false;
        }
        SQL_LOG(
            DEBUG, "create query executor, query executor [%s]", queryExecutor->toString().c_str());

        isearch::search::QueryExecutorPtr queryExecutorPtr(
            queryExecutor, [](isearch::search::QueryExecutor *p) { POOL_DELETE_CLASS(p); });
        if (!_useSubR->getUseSub() && queryExecutor->hasSubDocExecutor()) {
            SQL_LOG(ERROR,
                    "use sub query [%s] without unnest sub table",
                    queryExecutor->toString().c_str());
            return false;
        }
        queryExecutors.emplace_back(queryExecutorPtr);
    }
    return true;
}

isearch::search::QueryExecutor *
ScanIteratorCreatorR::createQueryExecutor(const isearch::common::QueryPtr &query,
                                          const std::string &mainTableName,
                                          isearch::search::LayerMeta *layerMeta,
                                          bool &emptyScan) {
    emptyScan = false;
    isearch::search::QueryExecutor *queryExecutor = NULL;
    auto pool = _queryMemPoolR->getPool().get();
    if (query == NULL) {
        queryExecutor = POOL_NEW_CLASS(pool, isearch::search::RangeQueryExecutor, layerMeta);
        SQL_LOG(TRACE1, "table name [%s], no query executor, null query", mainTableName.c_str());
        return queryExecutor;
    }
    SQL_LOG(DEBUG,
            "create query executor, table name [%s], query [%s]",
            mainTableName.c_str(),
            query->toString().c_str());
    isearch::ErrorCode errorCode = isearch::ERROR_NONE;
    std::string errorMsg;
    try {
        isearch::search::QueryExecutorCreator qeCreator(_matchDataManager.get(),
                                                        _indexPartitionReaderWrapper.get(),
                                                        pool,
                                                        _timeoutTerminatorR->getTimeoutTerminator(),
                                                        layerMeta);
        query->accept(&qeCreator);
        queryExecutor = qeCreator.stealQuery();
        if (queryExecutor->isEmpty()) {
            POOL_DELETE_CLASS(queryExecutor);
            queryExecutor = NULL;
            emptyScan = true;
        }
    } catch (const indexlib::util::ExceptionBase &e) {
        SQL_LOG(WARN, "table name [%s], lookup exception: [%s]", mainTableName.c_str(), e.what());
        errorMsg = "ExceptionBase: " + e.GetClassName();
        errorCode = isearch::ERROR_SEARCH_LOOKUP;
        if (e.GetClassName() == "FileIOException") {
            errorCode = isearch::ERROR_SEARCH_LOOKUP_FILEIO_ERROR;
        }
    } catch (const std::exception &e) {
        errorMsg = e.what();
        errorCode = isearch::ERROR_SEARCH_LOOKUP;
    } catch (...) {
        errorMsg = "Unknown Exception";
        errorCode = isearch::ERROR_SEARCH_LOOKUP;
    }
    if (errorCode != isearch::ERROR_NONE) {
        SQL_LOG(WARN,
                "table name [%s], Create query executor failed, query [%s], Exception: [%s]",
                mainTableName.c_str(),
                query->toString().c_str(),
                errorMsg.c_str());
    }
    return queryExecutor;
}

bool ScanIteratorCreatorR::createFilterWrapper(
    suez::turing::AttributeExpression *attrExpr,
    const std::vector<suez::turing::AttributeExpression *> &queryExprVec,
    suez::turing::JoinDocIdConverterCreator *joinDocIdConverterCreator,
    isearch::search::FilterWrapperPtr &filterWrapper) {
    auto pool = _queryMemPoolR->getPool().get();
    isearch::search::Filter *filter = NULL;
    isearch::search::SubDocFilter *subDocFilter = NULL;
    if (attrExpr) {
        suez::turing::AttributeExpressionTyped<bool> *boolAttrExpr
            = dynamic_cast<suez::turing::AttributeExpressionTyped<bool> *>(attrExpr);
        if (!boolAttrExpr) {
            SQL_LOG(WARN, "filter expression return type should be bool.");
            return false;
        }
        filter = POOL_NEW_CLASS(pool, isearch::search::Filter, boolAttrExpr);
        if (filter->needFilterSubDoc() && _matchDocAllocator->hasSubDocAllocator()) {
            subDocFilter = POOL_NEW_CLASS(
                pool, isearch::search::SubDocFilter, _matchDocAllocator->getSubDocAccessor());
        }
    }
    isearch::search::JoinFilter *joinFilter = NULL;
    if (joinDocIdConverterCreator != nullptr && !joinDocIdConverterCreator->isEmpty()) {
        joinFilter
            = POOL_NEW_CLASS(pool, isearch::search::JoinFilter, joinDocIdConverterCreator, true);
    }

    if (filter != NULL || subDocFilter != NULL || joinFilter != NULL) {
        if (queryExprVec.size() != 0) {
            filterWrapper.reset(new QueryExprFilterWrapper(queryExprVec));
        } else {
            filterWrapper.reset(new isearch::search::FilterWrapper());
        }
        filterWrapper->setFilter(filter);
        filterWrapper->setSubDocFilter(subDocFilter);
        filterWrapper->setJoinFilter(joinFilter);
    }
    return true;
}

isearch::search::LayerMetaPtr ScanIteratorCreatorR::createLayerMeta() {
    auto index = _scanInitParamR->parallelIndex;
    auto num = _scanInitParamR->parallelNum;
    auto limit = _scanInitParamR->limit;
    auto pool = _queryMemPoolR->getPool().get();
    isearch::search::LayerMeta fullLayerMeta
        = isearch::search::DefaultLayerMetaUtil::createFullRange(
            pool, _indexPartitionReaderWrapper.get());
    fullLayerMeta.quota = limit;
    isearch::search::LayerMetaPtr tmpLayerMeta(new isearch::search::LayerMeta(fullLayerMeta));
    if (num > 1) {
        tmpLayerMeta = splitLayerMeta(tmpLayerMeta, index, num);
    }
    return tmpLayerMeta;
}

void ScanIteratorCreatorR::proportionalLayerQuota(isearch::search::LayerMeta &layerMeta) {
    if (layerMeta.size() == 0 || layerMeta.quota == 0 || layerMeta.quotaMode == QM_PER_LAYER) {
        return;
    }

    uint32_t remainQuota = layerMeta.quota;
    docid_t totalRange = 0;
    for (size_t i = 0; i < layerMeta.size(); ++i) {
        auto &meta = layerMeta[i];
        layerMeta[i].quota = meta.end - meta.begin + 1;
        totalRange += layerMeta[i].quota;
    }
    if (totalRange > layerMeta.quota) {
        for (size_t i = 0; i < layerMeta.size(); ++i) {
            auto &meta = layerMeta[i];
            docid_t range = meta.end - meta.begin + 1;
            meta.quota = (uint32_t)((double)range * layerMeta.quota / totalRange);
            remainQuota -= meta.quota;
        }
        layerMeta[0].quota += remainQuota;
    }
    layerMeta.quota = 0;
}

isearch::search::LayerMetaPtr ScanIteratorCreatorR::splitLayerMeta(
    const isearch::search::LayerMetaPtr &layerMeta, uint32_t index, uint32_t num) {
    return splitLayerMeta(_queryMemPoolR->getPool().get(), layerMeta, index, num);
}

isearch::search::LayerMetaPtr
ScanIteratorCreatorR::splitLayerMeta(autil::mem_pool::Pool *pool,
                                     const isearch::search::LayerMetaPtr &layerMeta,
                                     uint32_t index,
                                     uint32_t num) {
    if (index >= num) {
        return {};
    }
    const auto &meta = *layerMeta;
    uint32_t df = 0;
    for (auto &rangeMeta : meta) {
        df += rangeMeta.end - rangeMeta.begin + 1;
    }
    uint32_t count = df / num;
    uint32_t left = df % num;
    uint32_t beginCount = count * index;
    if (index + 1 == num) {
        count += left;
    }
    isearch::search::LayerMetaPtr newMeta(new isearch::search::LayerMeta(pool));
    uint32_t leftCount = count;
    for (size_t i = 0; i < meta.size() && leftCount > 0; i++) {
        uint32_t rangeCount = meta[i].end - meta[i].begin + 1;
        if (beginCount >= rangeCount) {
            beginCount -= rangeCount;
            continue;
        } else {
            uint32_t newRangeBegin = meta[i].begin + beginCount;
            uint32_t rangeLeftCount = rangeCount - beginCount;
            uint32_t newRangeEnd = newRangeBegin;
            if (leftCount > rangeLeftCount) {
                newRangeEnd = meta[i].end;
                leftCount -= rangeLeftCount;
            } else {
                newRangeEnd = newRangeBegin + leftCount - 1;
                leftCount = 0;
            }
            isearch::search::DocIdRangeMeta newRangeMeta(
                newRangeBegin, newRangeEnd, meta[i].ordered, newRangeEnd - newRangeBegin + 1);
            newMeta->push_back(newRangeMeta);
            beginCount = 0;
        }
    }
    return newMeta;
}

ScanIterator *
ScanIteratorCreatorR::createDocIdScanIterator(const isearch::common::QueryPtr &query) {
    return new DocIdsScanIterator(
        _matchDocAllocator, _timeoutTerminatorR->getTimeoutTerminator(), query);
}

void ScanIteratorCreatorR::termCombine(
    isearch::common::QueryPtr &query,
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &tabletSchema) {
    isearch::common::TermCombineVisitor termCombineVisitor(
        tabletSchema->GetUserDefinedParam().GetMap());
    if (termCombineVisitor.hasCombineDict()) {
        SQL_LOG(TRACE1, "before term combine query [%s]", query->toString().c_str());
        query->accept(&termCombineVisitor);
        query.reset(termCombineVisitor.stealQuery());
        SQL_LOG(TRACE1, "after term combine query [%s]", query->toString().c_str());
    }
}

REGISTER_RESOURCE(ScanIteratorCreatorR);

} // namespace sql
