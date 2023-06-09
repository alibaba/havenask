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
#include "ha3/sql/ops/scan/ScanIteratorCreator.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <stdint.h>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryFlatten.h"
#include "ha3/common/QueryTermVisitor.h"
#include "ha3/common/Term.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/DocIdsQuery.h"
#include "ha3/isearch.h"
#include "ha3/search/AuxiliaryChainVisitor.h"
#include "ha3/search/DefaultLayerMetaUtil.h"
#include "ha3/search/Filter.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/QueryExecutorCreator.h"
#include "ha3/search/SubMatchCheckVisitor.h"
#include "ha3/search/SubMatchVisitor.h"
#include "ha3/search/RangeQueryExecutor.h"
#include "ha3/search/TermMatchVisitor.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/condition/Condition.h"
#include "ha3/sql/ops/condition/ConditionParser.h"
#include "ha3/sql/ops/scan/Ha3ScanConditionVisitor.h"
#include "ha3/sql/ops/scan/Ha3ScanIterator.h"
#include "ha3/sql/ops/scan/OrderedHa3ScanIterator.h"
#include "ha3/sql/ops/scan/QueryScanIterator.h"
#include "ha3/sql/ops/scan/RangeScanIterator.h"
#include "ha3/sql/ops/scan/RangeScanIteratorWithoutFilter.h"
#include "ha3/sql/ops/scan/QueryExprFilterWrapper.h"
#include "ha3/sql/ops/scan/Ha3ScanConditionVisitorParam.h"
#include "ha3/sql/ops/scan/DocIdsScanIterator.h"
#include "ha3/sql/ops/scan/QueryExecutorExpressionWrapper.h"
#include "ha3/sql/ops/scan/DocIdRangesReduceOptimize.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/Exception.h"
#include "indexlib/partition/index_partition_reader.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace indexlib {
namespace index {
class JoinDocidAttributeIterator;
} // namespace index
} // namespace indexlib
namespace isearch {
namespace sql {
class ScanIterator;
} // namespace sql
} // namespace isearch

using namespace std;
using namespace autil::mem_pool;
using namespace suez::turing;
using namespace matchdoc;
using namespace isearch::common;
using namespace isearch::search;
using namespace indexlib::index;
using namespace indexlib::config;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, ScanIteratorCreator);

ScanIteratorCreator::ScanIteratorCreator(const ScanIteratorCreatorParam &param)
    : _tableName(param.tableName)
    , _auxTableName(param.auxTableName)
    , _indexPartitionReaderWrapper(param.indexPartitionReaderWrapper)
    , _attributeExpressionCreator(param.attributeExpressionCreator)
    , _matchDocAllocator(param.matchDocAllocator)
    , _analyzerFactory(param.analyzerFactory)
    , _queryInfo(param.queryInfo)
    , _tableInfo(param.tableInfo)
    , _pool(param.pool)
    , _timeoutTerminator(param.timeoutTerminator)
    , _parallelIndex(param.parallelIndex)
    , _parallelNum(param.parallelNum)
    , _modelConfigMap(param.modelConfigMap)
    , _udfToQueryManager(param.udfToQueryManager)
    , _truncateDesc(param.truncateDesc)
    , _matchDataLabel(param.matchDataLabel)
    , _limit(param.limit)
    , _sortDesc(param.sortDesc)
    , _tableSortDescription(param.tableSortDescription)
    , _forbidIndexs(param.forbidIndexs)
{}

ScanIteratorCreator::~ScanIteratorCreator() {}

ScanIteratorPtr ScanIteratorCreator::createScanIterator(
        CreateScanIteratorInfo &info,
        bool useSub,
        bool &emptyScan)
{
    return createScanIterator(info.query, info.filterWrapper, info.queryExprs, info.layerMeta,
                              info.matchDataManager, useSub, emptyScan);
}

bool ScanIteratorCreator::genCreateScanIteratorInfo(
        const string &conditionInfo,
        const map<string, IndexInfo> &indexInfo,
        const map<string, FieldInfo> &fieldInfo,
        CreateScanIteratorInfo &info)
{
    ConditionParser parser(_pool);
    ConditionPtr condition;
    if (!parser.parseCondition(conditionInfo, condition)) {
        SQL_LOG(ERROR,
                "table name [%s],  parse condition [%s] failed",
                _tableName.c_str(),
                conditionInfo.c_str());
        return false;
    }
    LayerMetaPtr layerMeta = createLayerMeta(
            _indexPartitionReaderWrapper, _pool, _parallelIndex, _parallelNum, _limit);
    if (!layerMeta) {
        SQL_LOG(WARN, "table name [%s], create layer meta failed.", _tableName.c_str());
        return false;
    }
    SQL_LOG(DEBUG, "layer meta: %s", layerMeta->toString().c_str());
    if (_tableSortDescription != nullptr) {
        auto iter = _tableSortDescription->find(_tableName);
        if (iter != _tableSortDescription->end()) {
            SQL_LOG(DEBUG, "begin reduce docid range optimize");
            DocIdRangesReduceOptimize optimize(iter->second, fieldInfo);
            if (condition) {
                condition->accept(&optimize);
            }
            layerMeta = optimize.reduceDocIdRange(
                    layerMeta, _pool, _indexPartitionReaderWrapper);
            SQL_LOG(DEBUG, "after reduce docid range optimize, layer meta: %s",
                    layerMeta->toString().c_str());
        } else {
            SQL_LOG(DEBUG, "not find table [%s] sort description", _tableName.c_str());
        }
    }
    if (layerMeta) {
        layerMeta->quotaMode = QM_PER_DOC;
        proportionalLayerQuota(*layerMeta.get());
        SQL_LOG(DEBUG, "after proportional layer quota, layer meta: %s",
                layerMeta->toString().c_str());
    }

    Ha3ScanConditionVisitorParam param;
    param.attrExprCreator = _attributeExpressionCreator;
    param.indexPartitionReaderWrapper = _indexPartitionReaderWrapper;
    param.analyzerFactory = _analyzerFactory;
    param.indexInfo = &indexInfo;
    param.pool = _pool;
    param.queryInfo = _queryInfo;
    param.indexInfos = _tableInfo->getIndexInfos();
    param.mainTableName = _tableName;
    param.timeoutTerminator = _timeoutTerminator;
    param.layerMeta = layerMeta.get();
    param.modelConfigMap = _modelConfigMap;
    param.udfToQueryManager = _udfToQueryManager;
    param.forbidIndexs = _forbidIndexs;
    Ha3ScanConditionVisitor visitor(param);
    if (condition) {
        condition->accept(&visitor);
    }
    if (visitor.isError()) {
        SQL_LOG(WARN,
                "table name [%s], create scan iterator failed, error info [%s]",
                _tableName.c_str(),
                visitor.errorInfo().c_str());
        return false;
    }
    QueryPtr query(visitor.stealQuery());
    if (query) {
        SQL_LOG(TRACE1, "query [%s]", query->toString().c_str());
        QueryFlatten queryFlatten;
        queryFlatten.flatten(query.get());
        Query *flattenQuery = queryFlatten.stealQuery();
        assert(flattenQuery);
        query.reset(flattenQuery);
        SQL_LOG(TRACE1, "after flat query [%s]", query->toString().c_str());
        if (!_truncateDesc.empty()) {
            SQL_LOG(TRACE1, "use truncate query");
            try {
                truncateQuery(query);
            } catch (const autil::legacy::ExceptionBase &e) {
                SQL_LOG(ERROR, "scanInitParam init failed error:[%s].", e.what());
                return false;
            } catch (...) { return false; }
            SQL_LOG(TRACE1, "after truncate query [%s]", query->toString().c_str());
        }
        if (info.matchDataManager != nullptr && info.matchDataManager->needMatchData()) {
            SQL_LOG(TRACE1, "before sub match query");
            auto tabletSchema = _indexPartitionReaderWrapper->getSchema();
            if (!tabletSchema) {
                SQL_LOG(ERROR, "get index schema failed from table [%s]", _tableName.c_str());
                return false;
            }
            subMatchQuery(query, tabletSchema, _matchDataLabel);
            SQL_LOG(TRACE1, "after sub match query [%s]", query->toString().c_str());
        }
    }
    AttributeExpression *attrExpr = visitor.stealAttributeExpression();
    FilterWrapperPtr filterWrapper;
    if (attrExpr || !_auxTableName.empty()) {
        SQL_LOG(TRACE1, "condition expr [%s]", attrExpr->getOriginalString().c_str());
        if (!createJoinDocIdConverter()) {
            return false;
        }
        bool ret = createFilterWrapper(
                attrExpr, visitor.getQueryExprs(),
                _attributeExpressionCreator->getJoinDocIdConverterCreator(),
                _matchDocAllocator.get(), _pool, filterWrapper);
        if (!ret) {
            SQL_LOG(WARN,
                    "table name [%s], create filter wrapper failed, exprStr: [%s]",
                    _tableName.c_str(),
                    attrExpr->getOriginalString().c_str());
            return false;
        }
    }
    info.query = query;
    info.filterWrapper = filterWrapper;
    info.queryExprs = visitor.getQueryExprs();
    info.layerMeta = layerMeta;
    return true;
}

bool ScanIteratorCreator::createJoinDocIdConverter() {
    if (!_auxTableName.empty()) {
        if (!_attributeExpressionCreator->createJoinDocIdConverter(_auxTableName, _tableName))
        {
            SQL_LOG(WARN, "aux join create join doc id converter failed, aux [%s], main [%s]",
                    _auxTableName.c_str(), _tableName.c_str());
            return false;
        }
    }
    return true;
}

void ScanIteratorCreator::truncateQuery(QueryPtr &query) {
    vector<string> truncateNames;
    vector<SelectAuxChainType> types;
    if (!parseTruncateDesc(_truncateDesc, truncateNames, types)) {
        return;
    }
    for (size_t i = 0; i < truncateNames.size(); ++i) {
        TermDFMap termDFMap;
        if (types[i] != SAC_ALL) {
            QueryTermVisitor visitor(QueryTermVisitor::VT_ANDNOT_QUERY);
            query->accept(&visitor);
            const TermVector& termVector = visitor.getTermVector();
            for (const auto &term : termVector) {
                termDFMap[term] = _indexPartitionReaderWrapper->getTermDF(term);
            }
        }
        AuxiliaryChainVisitor visitor(truncateNames[i], termDFMap, types[i]);
        query->accept(&visitor);
    }
}

bool ScanIteratorCreator::parseTruncateDesc(
        const std::string &truncateDesc,
        vector<string> &truncateNames,
        vector<SelectAuxChainType> &types)
{
    for (const auto &auxChain : autil::StringUtil::split(truncateDesc, ";")) {
        SelectAuxChainType type = SelectAuxChainType::SAC_DF_SMALLER;
        string truncateName;
        for (const auto &option : autil::StringUtil::split(auxChain, "|")) {
            const std::vector<std::string> &st = autil::StringUtil::split(option, "#");
            if (st.size() != 2) {
                SQL_LOG(WARN, "Invalid truncate query option[%s], skip optimize",
                        option.c_str());
                return false;
            }
            if (st[0] == "select") {
                if (st[1] == "bigger" || st[1] == "BIGGER") {
                    type = SelectAuxChainType::SAC_DF_BIGGER;
                } else if (st[1] == "ALL" || st[1] == "all") {
                    type = SelectAuxChainType::SAC_ALL;
                } else if (st[1] == "smaller" || st[1] == "SMALLER") {
                    type = SelectAuxChainType::SAC_DF_SMALLER;
                } else {
                    SQL_LOG(WARN, "Invalid truncate query select type [%s], skip optimize",
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

void ScanIteratorCreator::subMatchQuery(QueryPtr &query,
                                        const std::shared_ptr<indexlibv2::config::ITabletSchema> &indexSchemaPtr,
                                        const std::string &matchDataLabel) {
    SubMatchCheckVisitor subMatchCheckVisitor;
    query->accept(&subMatchCheckVisitor);
    if (subMatchCheckVisitor.needSubMatch()) {
        SubMatchVisitor subMatchVisitor;
        query->accept(&subMatchVisitor);
        if (matchDataLabel == "full_term") {
            TermMatchVisitor termMatchVisitor(indexSchemaPtr);
            query->accept(&termMatchVisitor);
        }
    }
}

ScanIteratorPtr
ScanIteratorCreator::createScanIterator(const common::QueryPtr &query,
                                        const search::FilterWrapperPtr &filterWrapper,
                                        const vector<AttributeExpression *> &queryExprs,
                                        const search::LayerMetaPtr &layerMeta,
                                        const search::MatchDataManagerPtr &matchDataManager,
                                        bool useSub,
                                        bool &emptyScan)
{
    emptyScan = false;
    if (!layerMeta) {
        SQL_LOG(WARN, "table name [%s], layer meta is null.", _tableName.c_str());
        return NULL;
    }
    if (layerMeta->size() == 0) {
        SQL_LOG(TRACE1, "table name [%s], empty layer meta.", _tableName.c_str());
        emptyScan = true;
        return NULL;
    }
    std::vector<LayerMetaPtr> layerMetas;
    ScanIteratorPtr scanIterator = ScanIteratorPtr();
    if (_sortDesc.topk != 0) { // has sort desc
        SQL_LOG(TRACE2, "create ordered ha3 scan iter");
        for (size_t i = 0; i < layerMeta->size(); ++i) {
            LayerMetaPtr singleLayerMeta(new LayerMeta(*layerMeta));
            singleLayerMeta->clear();
            singleLayerMeta->push_back((*layerMeta)[i]);
            layerMetas.emplace_back(singleLayerMeta);
        }
        scanIterator.reset(createOrderedHa3ScanIterator(query,
                        filterWrapper,
                        _indexPartitionReaderWrapper,
                        _matchDocAllocator,
                        _timeoutTerminator,
                        layerMetas,
                        matchDataManager,
                        useSub,
                        emptyScan));
    } else if (dynamic_cast<DocIdsQuery *>(query.get()) != nullptr &&
               filterWrapper == nullptr)
    {
        SQL_LOG(TRACE2, "create docids scan iter");
        scanIterator.reset(createDocIdScanIterator(query));
    } else if (needHa3Scan(query, matchDataManager)) {
        SQL_LOG(TRACE2, "create ha3 scan iter");
        scanIterator.reset(createHa3ScanIterator(query,
                        filterWrapper,
                        _indexPartitionReaderWrapper,
                        _matchDocAllocator,
                        _timeoutTerminator,
                        layerMeta,
                        matchDataManager,
                        useSub,
                        emptyScan));
        layerMetas = {layerMeta};
    } else if (nullptr == query) {
        SQL_LOG(TRACE2, "create range scan iter");
        scanIterator.reset(createRangeScanIterator(filterWrapper, layerMeta));
        layerMetas = {layerMeta};
    } else {
        SQL_LOG(TRACE2, "create query scan iter");
        scanIterator.reset(createQueryScanIterator(
                        query, filterWrapper, layerMeta, useSub, emptyScan));
        layerMetas = {layerMeta};
    }
    for (auto *expr : queryExprs) {
        auto *queryExpr = dynamic_cast<QueryExecutorExpressionWrapper *>(expr);
        if (queryExpr == nullptr) {
            SQL_LOG(ERROR, "unexpected query expression cast failed, [%s]",
                    expr->getOriginalString().c_str());
            return ScanIteratorPtr();
        }
        if (!queryExpr->init(_indexPartitionReaderWrapper, _tableName, _pool,
                             _timeoutTerminator, layerMetas))
        {
            SQL_LOG(ERROR, "unexpected init query expression failed, [%s]",
                    expr->getOriginalString().c_str());
            return ScanIteratorPtr();
        }
    }
    return scanIterator;
}

bool ScanIteratorCreator::needHa3Scan(const common::QueryPtr &query,
                                      const search::MatchDataManagerPtr &matchDataManager)
{
    return _matchDocAllocator->hasSubDocAllocator() ||
        (matchDataManager != nullptr && matchDataManager->needMatchData()) ||
        (query != nullptr && _limit != std::numeric_limits<uint32_t>::max());
}

bool ScanIteratorCreator::isTermQuery(const QueryPtr &query) {
    if (query == NULL) {
        return false;
    }
    QueryType type = query->getType();
    if (type == TERM_QUERY || type == NUMBER_QUERY) {
        return true;
    }
    return false;
}

ScanIterator *ScanIteratorCreator::createRangeScanIterator(const FilterWrapperPtr &filterWrapper,
                                                           const LayerMetaPtr &layerMeta) {
    shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    delMapReader = _indexPartitionReaderWrapper->getDeletionMapReader();
    if (filterWrapper != NULL) {
        return new RangeScanIterator(
            filterWrapper, _matchDocAllocator, delMapReader, layerMeta, _timeoutTerminator);
    } else {
        return new RangeScanIteratorWithoutFilter(
            _matchDocAllocator, delMapReader, layerMeta, _timeoutTerminator);
    }
}

ScanIterator *ScanIteratorCreator::createQueryScanIterator(const QueryPtr &query,
        const FilterWrapperPtr &filterWrapper,
        const LayerMetaPtr &layerMeta,
        bool useSub,
        bool &emptyScan)
{
    QueryExecutor *queryExecutor = createQueryExecutor(query,
            _indexPartitionReaderWrapper,
            _tableName,
            _pool,
            NULL,
            layerMeta.get(),
            nullptr,
            emptyScan);
    if (emptyScan) {
        SQL_LOG(TRACE1, "empty result scan condition. query [%s]", query->toString().c_str());
        return NULL;
    }
    if (!queryExecutor) {
        SQL_LOG(WARN,
                "table name [%s], create query executor failed. query [%s]",
                _tableName.c_str(),
                query->toString().c_str());
        return NULL;
    }
    QueryExecutorPtr queryExecutorPtr(queryExecutor,
                                      [](QueryExecutor *p) { POOL_DELETE_CLASS(p); });
    if (!useSub && queryExecutor->hasSubDocExecutor()) {
        SQL_LOG(ERROR, "use sub query [%s] without unnest sub table",
                queryExecutor->toString().c_str());
        return NULL;
    }

    shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    delMapReader = _indexPartitionReaderWrapper->getDeletionMapReader();
    return new QueryScanIterator(queryExecutorPtr,
                                 filterWrapper,
                                 _matchDocAllocator,
                                 delMapReader,
                                 layerMeta,
                                 _timeoutTerminator);
}

ScanIterator *ScanIteratorCreator::createHa3ScanIterator(
    const QueryPtr &query,
    const FilterWrapperPtr &filterWrapper,
    IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper,
    const MatchDocAllocatorPtr &matchDocAllocator,
    autil::TimeoutTerminator *timeoutTerminator,
    const LayerMetaPtr &layerMeta,
    const search::MatchDataManagerPtr &matchDataManager,
    bool useSub,
    bool &emptyScan)
{
    vector<QueryExecutorPtr> queryExecutors;
    if (!createQueryExecutors(query, indexPartitionReaderWrapper,
                              _tableName, _pool, timeoutTerminator,
                              {layerMeta}, matchDataManager.get(), useSub,
                              emptyScan, queryExecutors))
    {
        SQL_LOG(TRACE1, "create query executors failed");
        return nullptr;
    }

    shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    indexlib::index::DeletionMapReaderPtr subDelMapReader;
    indexlib::index::JoinDocidAttributeIterator *mainToSubIt = NULL;
    delMapReader = indexPartitionReaderWrapper->getDeletionMapReader();

    if (matchDocAllocator->hasSubDocAllocator()) {
        mainToSubIt = indexPartitionReaderWrapper->getMainToSubIter();
        const indexlib::partition::IndexPartitionReaderPtr &subIndexPartReader
            = indexPartitionReaderWrapper->getSubReader();
        if (subIndexPartReader) {
            subDelMapReader = subIndexPartReader->GetDeletionMapReader();
        }
    }

    Ha3ScanIteratorParam seekParam;
    seekParam.queryExecutors = queryExecutors;
    seekParam.filterWrapper = filterWrapper;
    seekParam.matchDocAllocator = matchDocAllocator;
    seekParam.delMapReader = delMapReader;
    seekParam.subDelMapReader = subDelMapReader;
    seekParam.layerMetas = {layerMeta};
    if (query != nullptr) {
        seekParam.matchDataManager = matchDataManager;
    }
    seekParam.mainToSubIt = mainToSubIt;
    seekParam.timeoutTerminator = timeoutTerminator;
    seekParam.needAllSubDocFlag = false;
    return new Ha3ScanIterator(seekParam);
}

ScanIterator *ScanIteratorCreator::createOrderedHa3ScanIterator(
    const QueryPtr &query,
    const FilterWrapperPtr &filterWrapper,
    IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper,
    const MatchDocAllocatorPtr &matchDocAllocator,
    autil::TimeoutTerminator *timeoutTerminator,
    const std::vector<LayerMetaPtr> &layerMetas,
    const search::MatchDataManagerPtr &matchDataManager,
    bool useSub,
    bool &emptyScan)
{
    if (_matchDocAllocator->hasSubDocAllocator() || useSub) {
        SQL_LOG(ERROR, "unexpected ordered scan iter has UNNEST table");
        return nullptr;
    }

    vector<QueryExecutorPtr> queryExecutors;
    if (!createQueryExecutors(query, indexPartitionReaderWrapper,
                              _tableName, _pool, timeoutTerminator,
                              layerMetas, matchDataManager.get(), useSub,
                              emptyScan, queryExecutors))
    {
        SQL_LOG(TRACE1, "create query executors failed");
        return nullptr;
    }

    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    delMapReader = indexPartitionReaderWrapper->getDeletionMapReader();

    Ha3ScanIteratorParam seekParam;
    seekParam.queryExecutors = queryExecutors;
    seekParam.filterWrapper = filterWrapper;
    seekParam.matchDocAllocator = matchDocAllocator;
    seekParam.delMapReader = delMapReader;
    seekParam.subDelMapReader = nullptr;
    seekParam.layerMetas = layerMetas;
    if (query != nullptr) {
        seekParam.matchDataManager = matchDataManager;
    }
    seekParam.mainToSubIt = nullptr;
    seekParam.timeoutTerminator = timeoutTerminator;
    seekParam.needAllSubDocFlag = false;
    OrderedHa3ScanIterator *orderedHa3ScanIterator(new OrderedHa3ScanIterator(
                    seekParam, _pool, _sortDesc, _attributeExpressionCreator.get()));
    if (!orderedHa3ScanIterator->init()) {
        SQL_LOG(WARN, "create orderedHa3ScanIterator failed");
        delete orderedHa3ScanIterator;
        return nullptr;
    }
    return orderedHa3ScanIterator;
}

bool ScanIteratorCreator::createQueryExecutors(const QueryPtr &query,
        IndexPartitionReaderWrapperPtr &indexPartitionReader,
        const std::string &mainTableName,
        autil::mem_pool::Pool *pool,
        autil::TimeoutTerminator *timeoutTerminator,
        const std::vector<LayerMetaPtr> &layerMetas,
        MatchDataManager *matchDataManager,
        bool useSub,
        bool &emptyScan,
        std::vector<QueryExecutorPtr> &queryExecutors)
{
    for (size_t i = 0; i < layerMetas.size(); ++i) {
        QueryExecutor *queryExecutor = createQueryExecutor(query,
                indexPartitionReader,
                mainTableName,
                pool,
                timeoutTerminator,
                layerMetas[i].get(),
                matchDataManager,
                emptyScan);
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
        SQL_LOG(DEBUG, "create query executor, query executor [%s]",
                queryExecutor->toString().c_str());

        QueryExecutorPtr queryExecutorPtr(queryExecutor,
                [](QueryExecutor *p) { POOL_DELETE_CLASS(p); });
        if (!useSub && queryExecutor->hasSubDocExecutor()) {
            SQL_LOG(ERROR, "use sub query [%s] without unnest sub table",
                    queryExecutor->toString().c_str());
            return false;
        }
        queryExecutors.emplace_back(queryExecutorPtr);
    }
    return true;
}

QueryExecutor *
ScanIteratorCreator::createQueryExecutor(const QueryPtr &query,
                                         IndexPartitionReaderWrapperPtr &indexPartitionReader,
                                         const std::string &mainTableName,
                                         autil::mem_pool::Pool *pool,
                                         autil::TimeoutTerminator *timeoutTerminator,
                                         LayerMeta *layerMeta,
                                         MatchDataManager *matchDataManager,
                                         bool &emptyScan)
{
    emptyScan = false;
    QueryExecutor *queryExecutor = NULL;
    if (query == NULL) {
        queryExecutor = POOL_NEW_CLASS(pool, RangeQueryExecutor, layerMeta);
        SQL_LOG(TRACE1, "table name [%s], no query executor, null query", mainTableName.c_str());
        return queryExecutor;
    }
    SQL_LOG(DEBUG, "create query executor, table name [%s], query [%s]",
            mainTableName.c_str(),
            query->toString().c_str());
    ErrorCode errorCode = ERROR_NONE;
    std::string errorMsg;
    try {
        QueryExecutorCreator qeCreator(
            matchDataManager, indexPartitionReader.get(), pool, timeoutTerminator, layerMeta);
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
        errorCode = ERROR_SEARCH_LOOKUP;
        if (e.GetClassName() == "FileIOException") {
            errorCode = ERROR_SEARCH_LOOKUP_FILEIO_ERROR;
        }
    } catch (const std::exception &e) {
        errorMsg = e.what();
        errorCode = ERROR_SEARCH_LOOKUP;
    } catch (...) {
        errorMsg = "Unknown Exception";
        errorCode = ERROR_SEARCH_LOOKUP;
    }
    if (errorCode != ERROR_NONE) {
        SQL_LOG(WARN,
                "table name [%s], Create query executor failed, query [%s], Exception: [%s]",
                mainTableName.c_str(),
                query->toString().c_str(),
                errorMsg.c_str());
    }
    return queryExecutor;
}

bool ScanIteratorCreator::createFilterWrapper(AttributeExpression *attrExpr,
        const std::vector<suez::turing::AttributeExpression *> &queryExprVec,
        JoinDocIdConverterCreator *joinDocIdConverterCreator,
        MatchDocAllocator *matchDocAllocator,
        autil::mem_pool::Pool *pool,
        FilterWrapperPtr &filterWrapper)
{
    Filter *filter = NULL;
    SubDocFilter *subDocFilter = NULL;
    if (attrExpr) {
        AttributeExpressionTyped<bool> *boolAttrExpr
            = dynamic_cast<AttributeExpressionTyped<bool> *>(attrExpr);
        if (!boolAttrExpr) {
            SQL_LOG(WARN, "filter expression return type should be bool.");
            return false;
        }
        filter = POOL_NEW_CLASS(pool, Filter, boolAttrExpr);
        if (filter->needFilterSubDoc() && matchDocAllocator->hasSubDocAllocator()) {
            subDocFilter = POOL_NEW_CLASS(pool, SubDocFilter, matchDocAllocator->getSubDocAccessor());
        }
    }
    JoinFilter *joinFilter = NULL;
    if (joinDocIdConverterCreator != nullptr && !joinDocIdConverterCreator->isEmpty()) {
        joinFilter = POOL_NEW_CLASS(pool, JoinFilter,
                joinDocIdConverterCreator, true);
    }

    if (filter != NULL || subDocFilter != NULL || joinFilter != NULL) {
        if (queryExprVec.size() != 0) {
            filterWrapper.reset(new QueryExprFilterWrapper(queryExprVec));
        } else {
            filterWrapper.reset(new FilterWrapper());
        }
        filterWrapper->setFilter(filter);
        filterWrapper->setSubDocFilter(subDocFilter);
        filterWrapper->setJoinFilter(joinFilter);
    }
    return true;
}

LayerMetaPtr
ScanIteratorCreator::createLayerMeta(IndexPartitionReaderWrapperPtr &indexPartitionReader,
                                     autil::mem_pool::Pool *pool,
                                     uint32_t index,
                                     uint32_t num,
                                     uint32_t limit)
{
    LayerMeta fullLayerMeta
        = DefaultLayerMetaUtil::createFullRange(pool, indexPartitionReader.get());
    fullLayerMeta.quota = limit;
    LayerMetaPtr tmpLayerMeta(new LayerMeta(fullLayerMeta));
    if (num > 1) {
        tmpLayerMeta = splitLayerMeta(pool, tmpLayerMeta, index, num);
    }
    return tmpLayerMeta;
}

void ScanIteratorCreator::proportionalLayerQuota(LayerMeta &layerMeta) {
    if (layerMeta.size() == 0
        || layerMeta.quota == 0
        || layerMeta.quotaMode == QM_PER_LAYER)
    {
        return;
    }

    uint32_t remainQuota = layerMeta.quota;
    docid_t totalRange = 0;
    for (size_t i = 0; i < layerMeta.size(); ++i) {
        DocIdRangeMeta &meta = layerMeta[i];
        layerMeta[i].quota = meta.end - meta.begin + 1;
        totalRange += layerMeta[i].quota;
    }
    if (totalRange > layerMeta.quota) {
        for (size_t i = 0; i < layerMeta.size(); ++i) {
            DocIdRangeMeta &meta = layerMeta[i];
            docid_t range = meta.end - meta.begin + 1;
            meta.quota = (uint32_t) ((double)range * layerMeta.quota / totalRange);
            remainQuota -= meta.quota;
        }
        layerMeta[0].quota += remainQuota;
    }
    layerMeta.quota = 0;
}

LayerMetaPtr ScanIteratorCreator::splitLayerMeta(autil::mem_pool::Pool *pool,
                                                 const LayerMetaPtr &layerMeta,
                                                 uint32_t index,
                                                 uint32_t num)
{
    if (index >= num) {
        return {};
    }
    const LayerMeta &meta = *layerMeta;
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
    LayerMetaPtr newMeta(new LayerMeta(pool));
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
            DocIdRangeMeta newRangeMeta(newRangeBegin, newRangeEnd,
                    meta[i].ordered, newRangeEnd - newRangeBegin + 1);
            newMeta->push_back(newRangeMeta);
            beginCount = 0;
        }
    }
    return newMeta;
}

ScanIterator *ScanIteratorCreator::createDocIdScanIterator(const QueryPtr &query) {
    return new DocIdsScanIterator(_matchDocAllocator, _timeoutTerminator, query);
}

} // namespace sql
} // namespace isearch
