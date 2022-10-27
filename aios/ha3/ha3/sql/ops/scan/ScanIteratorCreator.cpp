#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/LayerMetasCreator.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/ops/scan/Ha3ScanConditionVisitor.h>
#include <ha3/sql/ops/scan/RangeQueryExecutor.h>
#include <ha3/sql/ops/scan/ScanIteratorCreator.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/ops/scan/Ha3ScanIterator.h>
#include <ha3/sql/ops/scan/RangeScanIterator.h>
#include <ha3/sql/ops/scan/RangeScanIteratorWithoutFilter.h>
#include <ha3/sql/ops/scan/QueryScanIterator.h>
#include <ha3/sql/common/common.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <suez/turing/expression/provider/FunctionProvider.h>
#include <indexlib/misc/exception.h>
#include <indexlib/partition/index_partition_reader.h>
#include <memory>
#include <indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h>
#include <ha3/qrs/QueryFlatten.h>
using namespace std;
using namespace autil_rapidjson;
using namespace autil::mem_pool;
using namespace suez::turing;
using namespace matchdoc;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(qrs);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, ScanIteratorCreator);

ScanIteratorCreator::ScanIteratorCreator(const ScanIteratorCreatorParam &param)
    : _tableName(param.tableName)
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
{}

ScanIteratorCreator::~ScanIteratorCreator() {
}

ScanIterator* ScanIteratorCreator::createScanIterator(
        const std::string &conditionInfo,
        const map<string, IndexInfo> &indexInfo,
        bool &emptyScan)
{
    CreateScanIteratorInfo createInfo;
    if (!genCreateScanIteratorInfo(conditionInfo, indexInfo, createInfo)) {
        return NULL;
    }
    return createScanIterator(createInfo, emptyScan);
}

ScanIterator *ScanIteratorCreator::createScanIterator(CreateScanIteratorInfo &info, bool &emptyScan)
{
    return createScanIterator(info.query, info.filterWrapper, info.layerMeta, emptyScan);
}

bool ScanIteratorCreator::genCreateScanIteratorInfo(const string &conditionInfo,
        const map<string, IndexInfo> &indexInfo,
        CreateScanIteratorInfo &info)
{
    ConditionParser parser(_pool);
    ConditionPtr condition;
    if (!parser.parseCondition(conditionInfo, condition)) {
        SQL_LOG(ERROR, "table name [%s],  parse condition [%s] failed",
                        _tableName.c_str(), conditionInfo.c_str());
        return false;
    }
    LayerMetaPtr layerMeta = createLayerMeta(_indexPartitionReaderWrapper, _pool, _parallelIndex, _parallelNum);
    if (!layerMeta) {
        SQL_LOG(WARN, "table name [%s], create layer meta failed.", _tableName.c_str());
        return false;
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
    Ha3ScanConditionVisitor visitor(param);
    if (condition) {
        condition->accept(&visitor);
    }
    if (visitor.isError()) {
        SQL_LOG(WARN, "table name [%s], create scan iterator failed, error info [%s]",
                        _tableName.c_str(), visitor.errorInfo().c_str());
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
    }
    AttributeExpression *attrExpr = visitor.stealAttributeExpression();
    FilterWrapperPtr filterWrapper;
    if (attrExpr) {
        SQL_LOG(TRACE1, "condition expr [%s]", attrExpr->getOriginalString().c_str());
        bool ret = createFilterWrapper(attrExpr, _matchDocAllocator.get(), _pool, filterWrapper);
        if (!ret) {
            SQL_LOG(WARN, "table name [%s], create filter wrapper failed, exprStr: [%s]",
                            _tableName.c_str(), attrExpr->getOriginalString().c_str());
            return false;
        }
    }
    info.query = query;
    info.filterWrapper = filterWrapper;
    info.layerMeta = layerMeta;
    return true;
}

ScanIterator *ScanIteratorCreator::createScanIterator(
        const common::QueryPtr &query,
        const search::FilterWrapperPtr &filterWrapper,
        const search::LayerMetaPtr &layerMeta,
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
    if (!_matchDocAllocator->hasSubDocAllocator()) {
        if (query == NULL) {
            return createRangeScanIterator(filterWrapper, layerMeta);
        } else {
            return createQueryScanIterator(query, filterWrapper, layerMeta, emptyScan);
        }
    } else {
        return createHa3ScanIterator(query, filterWrapper,
                _indexPartitionReaderWrapper, _matchDocAllocator,
                _timeoutTerminator, layerMeta, emptyScan);
    }
}

bool ScanIteratorCreator::isTermQuery(const QueryPtr &query) {
    if (query == NULL) {
        return false;
    }
    QueryType type = query->getType();
    if ( type == TERM_QUERY || type == NUMBER_QUERY) {
        return true;
    }
    return false;
}

ScanIterator* ScanIteratorCreator::createRangeScanIterator(
        const FilterWrapperPtr &filterWrapper,
        const LayerMetaPtr &layerMeta)
{
    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    auto indexPartitionReader = _indexPartitionReaderWrapper->getReader();
    delMapReader = indexPartitionReader->GetDeletionMapReader();
    if (filterWrapper != NULL) {
        return new RangeScanIterator(filterWrapper, _matchDocAllocator, delMapReader, layerMeta, _timeoutTerminator);
    } else {
        return new RangeScanIteratorWithoutFilter(_matchDocAllocator, delMapReader, layerMeta, _timeoutTerminator);
    }
}

ScanIterator* ScanIteratorCreator::createQueryScanIterator(
        const QueryPtr &query,
        const FilterWrapperPtr &filterWrapper,
        const LayerMetaPtr &layerMeta,
        bool &emptyScan)
{
    QueryExecutor* queryExecutor =
        createQueryExecutor(query, _indexPartitionReaderWrapper,
                            _tableName, _pool, NULL, layerMeta.get(), emptyScan);
    if (emptyScan) {
        SQL_LOG(TRACE1, "empty result scan condition. query [%s]", query->toString().c_str());
        return NULL;
    }
    if (!queryExecutor) {
        SQL_LOG(WARN, "table name [%s], create query executor failed. query [%s]",
                        _tableName.c_str(), query->toString().c_str());
        return NULL;
    }
    QueryExecutorPtr queryExecutorPtr(queryExecutor,
            [](QueryExecutor *p) {
                                      POOL_DELETE_CLASS(p);
                                  });

    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    auto indexPartitionReader = _indexPartitionReaderWrapper->getReader();
    delMapReader = indexPartitionReader->GetDeletionMapReader();
    return new QueryScanIterator(queryExecutorPtr, filterWrapper, _matchDocAllocator,
                                 delMapReader, layerMeta, _timeoutTerminator);
}

ScanIterator* ScanIteratorCreator::createHa3ScanIterator(
        const QueryPtr &query,
        const FilterWrapperPtr &filterWrapper,
        const IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper,
        const MatchDocAllocatorPtr &matchDocAllocator,
        TimeoutTerminator* timeoutTerminator,
        const LayerMetaPtr &layerMeta,
        bool &emptyScan)
{
    QueryExecutor* queryExecutor =
        createQueryExecutor(query, _indexPartitionReaderWrapper,
                            _tableName, _pool, timeoutTerminator, layerMeta.get(), emptyScan);
    if (emptyScan) {
        SQL_LOG(TRACE1, "table name [%s], empty result scan condition. query [%s]",
                        _tableName.c_str(), query->toString().c_str());
        return NULL;
    }
    if (!queryExecutor) {
        SQL_LOG(WARN, "table name [%s], create query executor failed. query [%s]",
                        _tableName.c_str(), query->toString().c_str());
        return NULL;
    }
    QueryExecutorPtr queryExecutorPtr(queryExecutor,
            [](QueryExecutor *p) {
                                      POOL_DELETE_CLASS(p);
                                  });

    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    IE_NAMESPACE(index)::DeletionMapReaderPtr subDelMapReader;
    IE_NAMESPACE(index)::JoinDocidAttributeIterator *mainToSubIt = NULL;
    auto indexPartitionReader = indexPartitionReaderWrapper->getReader();
    delMapReader = indexPartitionReader->GetDeletionMapReader();

    if (matchDocAllocator->hasSubDocAllocator()) {
        mainToSubIt = indexPartitionReaderWrapper->getMainToSubIter();
        const IE_NAMESPACE(partition)::IndexPartitionReaderPtr &subIndexPartReader =
            indexPartitionReaderWrapper->getSubReader();
        if (subIndexPartReader) {
            subDelMapReader = subIndexPartReader->GetDeletionMapReader();
        }
    }

    Ha3ScanIteratorParam seekParam;
    seekParam.queryExecutor = queryExecutorPtr;
    seekParam.filterWrapper = filterWrapper;
    seekParam.matchDocAllocator = matchDocAllocator;
    seekParam.delMapReader = delMapReader;
    seekParam.subDelMapReader = subDelMapReader;
    seekParam.layerMeta = layerMeta;
    seekParam.mainToSubIt = mainToSubIt;
    seekParam.timeoutTerminator = timeoutTerminator;
    seekParam.needAllSubDocFlag = false;
    return new Ha3ScanIterator(seekParam);
}

QueryExecutor* ScanIteratorCreator::createQueryExecutor(const QueryPtr &query,
        IndexPartitionReaderWrapperPtr &indexPartitionReader,
        const std::string &mainTableName,
        autil::mem_pool::Pool *pool,
        TimeoutTerminator *timeoutTerminator,
        LayerMeta *layerMeta, bool &emptyScan)
{
    emptyScan = false;
    QueryExecutor *queryExecutor = NULL;
    if (query == NULL) {
        queryExecutor = POOL_NEW_CLASS(pool, RangeQueryExecutor, layerMeta);
        SQL_LOG(TRACE1, "table name [%s], no query executor, null query", mainTableName.c_str());
        return queryExecutor;
    }
    ErrorCode errorCode = ERROR_NONE;
    std::string errorMsg;
    try {
        QueryExecutorCreator qeCreator(NULL, indexPartitionReader.get(),
                pool, timeoutTerminator, layerMeta);
        query->accept(&qeCreator);
        queryExecutor = qeCreator.stealQuery();
        if (queryExecutor->isEmpty()) {
            POOL_DELETE_CLASS(queryExecutor);
            queryExecutor = NULL;
            emptyScan = true;
        }
    } catch (const IE_NAMESPACE(misc)::ExceptionBase &e) {
        SQL_LOG(WARN, "table name [%s], lookup exception: [%s]", mainTableName.c_str(), e.what());
        errorMsg = "ExceptionBase: " + e.GetClassName();
        errorCode = ERROR_SEARCH_LOOKUP;
        if (e.GetClassName() == "FileIOException") {
            errorCode = ERROR_SEARCH_LOOKUP_FILEIO_ERROR;
        }
    } catch (const std::exception& e) {
        errorMsg = e.what();
        errorCode = ERROR_SEARCH_LOOKUP;
    } catch (...) {
        errorMsg = "Unknown Exception";
        errorCode = ERROR_SEARCH_LOOKUP;
    }
    if (errorCode != ERROR_NONE) {
        SQL_LOG(WARN, "table name [%s], Create query executor failed, query [%s], Exception: [%s]",
                        mainTableName.c_str(), query->toString().c_str(), errorMsg.c_str());
    }
    return queryExecutor;
}

bool ScanIteratorCreator::createFilterWrapper(
        AttributeExpression* attrExpr,
        MatchDocAllocator *matchDocAllocator,
        autil::mem_pool::Pool *pool, FilterWrapperPtr &filterWrapper)
{
    Filter *filter = NULL;
    SubDocFilter *subDocFilter = NULL;

    AttributeExpressionTyped<bool> *boolAttrExpr =
        dynamic_cast<AttributeExpressionTyped<bool>*>(attrExpr);
    if (!boolAttrExpr) {
        SQL_LOG(WARN, "filter expression return type should be bool.");
        return false;
    }
    filter =  POOL_NEW_CLASS(pool, Filter, boolAttrExpr);
    if (filter->needFilterSubDoc() && matchDocAllocator->hasSubDocAllocator()) {
        subDocFilter = POOL_NEW_CLASS(pool, SubDocFilter,
                matchDocAllocator->getSubDocAccessor());
    }
    if (filter != NULL || subDocFilter != NULL) {
        filterWrapper.reset(new FilterWrapper());
        filterWrapper->setFilter(filter);
        filterWrapper->setSubDocFilter(subDocFilter);
    }
    return true;
}


LayerMetaPtr ScanIteratorCreator::createLayerMeta(
        IndexPartitionReaderWrapperPtr &indexPartitionReader,
        autil::mem_pool::Pool *pool, uint32_t index, uint32_t num)
{
    LayerMetasCreator layerMetasCreator;
    layerMetasCreator.init(pool, indexPartitionReader.get());
    LayerMetasPtr layerMetas(layerMetasCreator.create(NULL),
                             [](LayerMetas* p) {
                POOL_DELETE_CLASS(p);
            });
    LayerMetaPtr layerMeta;
    if (layerMetas == NULL) {
        SQL_LOG(INFO, "create layer meta failed.");
        return layerMeta;
    }
    if (layerMetas->size() != 1) {
        SQL_LOG(INFO, "layer meta size [%zu] not equal 1.", layerMetas->size());
        return layerMeta;
    }

    for (auto &rangeMeta : (*layerMetas)[0]) {
        rangeMeta.quota =  rangeMeta.end - rangeMeta.begin + 1;
    }
    LayerMetaPtr tmpLayerMeta(new LayerMeta((*layerMetas)[0]));
    if (num > 1) {
        tmpLayerMeta = splitLayerMeta(pool, tmpLayerMeta, index, num);
    }
    if (tmpLayerMeta) {
        tmpLayerMeta->quotaMode = QM_PER_LAYER;
    }
    return tmpLayerMeta;
}

LayerMetaPtr ScanIteratorCreator::splitLayerMeta(autil::mem_pool::Pool *pool,
        const LayerMetaPtr& layerMeta, uint32_t index, uint32_t num)
{
    if (index >= num) {
        return {};
    }
    const LayerMeta &meta = *layerMeta;
    uint32_t df = 0;
    for (auto &rangeMeta : meta) {
        df +=  rangeMeta.end - rangeMeta.begin + 1;
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
            DocIdRangeMeta newRangeMeta(newRangeBegin, newRangeEnd, newRangeEnd - newRangeBegin +1);
            newMeta->push_back(newRangeMeta);
            beginCount = 0;
        }
    }
    return newMeta;
}


END_HA3_NAMESPACE(sql);
