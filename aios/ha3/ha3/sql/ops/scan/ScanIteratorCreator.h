#ifndef ISEARCH_HA3SCANITERATORCREATOR_H
#define ISEARCH_HA3SCANITERATORCREATOR_H

#include <ha3/sql/common/common.h>
#include <ha3/common/Query.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/sql/ops/scan/ScanIterator.h>
#include <ha3/sql/attr/IndexInfo.h>
#include <ha3/config/QueryInfo.h>
#include <matchdoc/MatchDocAllocator.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <suez/turing/common/TimeoutTerminator.h>
#include <indexlib/partition/partition_reader_snapshot.h>
#include <build_service/analyzer/AnalyzerFactory.h>

BEGIN_HA3_NAMESPACE(sql);

class ScanIterator;

struct ScanIteratorCreatorParam{
    ScanIteratorCreatorParam()
        : analyzerFactory(NULL)
        , queryInfo(NULL)
        , pool(NULL)
        , timeoutTerminator(NULL)
        , parallelIndex(0)
        , parallelNum(1)
    {}
    std::string tableName;
    search::IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    suez::turing::AttributeExpressionCreatorPtr attributeExpressionCreator;
    matchdoc::MatchDocAllocatorPtr matchDocAllocator;
    const build_service::analyzer::AnalyzerFactory *analyzerFactory;
    const config::QueryInfo *queryInfo;
    suez::turing::TableInfoPtr tableInfo;
    autil::mem_pool::Pool *pool;
    suez::turing::TimeoutTerminator *timeoutTerminator;
    uint32_t parallelIndex;
    uint32_t parallelNum;
};

struct CreateScanIteratorInfo {
    common::QueryPtr query;
    search::FilterWrapperPtr filterWrapper;
    search::LayerMetaPtr layerMeta;
};

class ScanIteratorCreator
{
public:
    ScanIteratorCreator(const ScanIteratorCreatorParam &param);
    ~ScanIteratorCreator();
private:
    ScanIteratorCreator(const ScanIteratorCreator &);
    ScanIteratorCreator& operator=(const ScanIteratorCreator &);
public:
    bool genCreateScanIteratorInfo(const std::string &conditionInfo,
                                   const std::map<std::string, IndexInfo> &indexInfo,
                                   CreateScanIteratorInfo &info);
    ScanIterator *createScanIterator(const std::string &conditionInfo,
            const std::map<std::string, IndexInfo> &indexInfo,
            bool &emptyScan);
    ScanIterator *createScanIterator(CreateScanIteratorInfo &info, bool &emptyScan);
    ScanIterator *createScanIterator(const common::QueryPtr &query,
            const search::FilterWrapperPtr &filterWrapper,
            const search::LayerMetaPtr &layerMeta,
            bool &emptyScan);

private:
    ScanIterator* createRangeScanIterator(const search::FilterWrapperPtr &filterWrapper,
            const search::LayerMetaPtr &layerMeta);
    ScanIterator* createHa3ScanIterator(const common::QueryPtr &query,
            const search::FilterWrapperPtr &filterWrapper,
            const search::IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper,
            const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
            suez::turing::TimeoutTerminator* timeoutTerminator,
            const search::LayerMetaPtr &layerMeta,
            bool &emptyScan);
    ScanIterator* createQueryScanIterator(const common::QueryPtr &query,
            const search::FilterWrapperPtr &filterWrapper,
            const search::LayerMetaPtr &layerMeta,
            bool &emptyScan);

private:
    bool isTermQuery(const common::QueryPtr &query);
    static search::QueryExecutor* createQueryExecutor(const common::QueryPtr &query,
            search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
            const std::string &mainTableName,
            autil::mem_pool::Pool *pool,
            suez::turing::TimeoutTerminator *timeoutTerminator,
            search::LayerMeta *layerMeta, bool &emptyScan);

    static search::LayerMetaPtr createLayerMeta(
            search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
            autil::mem_pool::Pool *pool, uint32_t index = 0, uint32_t num = 1);
    static search::LayerMetaPtr splitLayerMeta(
            autil::mem_pool::Pool *pool,
            const search::LayerMetaPtr& layerMeta, uint32_t index, uint32_t num);

    static bool parseIndexMap(const std::string &indexStr, autil::mem_pool::Pool* pool,
                              std::map<std::string, std::string> &attrIndexMap);

    static bool createFilterWrapper(suez::turing::AttributeExpression* attrExpr,
                                    matchdoc::MatchDocAllocator *matchDocAllocator,
                                    autil::mem_pool::Pool *pool, search::FilterWrapperPtr &filterWrapper);

private:
    std::string _tableName;
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    const build_service::analyzer::AnalyzerFactory *_analyzerFactory;
    const config::QueryInfo *_queryInfo;
    suez::turing::TableInfoPtr _tableInfo;
    autil::mem_pool::Pool *_pool;
    suez::turing::TimeoutTerminator *_timeoutTerminator;
    uint32_t _parallelIndex;
    uint32_t _parallelNum;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ScanIteratorCreator);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_HA3SCANITERATORCREATOR_H
