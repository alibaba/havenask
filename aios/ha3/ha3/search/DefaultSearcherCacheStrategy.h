#ifndef ISEARCH_DEFAULTSEARCHERCACHESTRATEGY_H
#define ISEARCH_DEFAULTSEARCHERCACHESTRATEGY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SearcherCacheStrategy.h>
#include <ha3/common/SearcherCacheClause.h>
#include <indexlib/partition/index_partition_reader.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3/search/Filter.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/common/MatchDocs.h>
#include <ha3/common/Result.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/func_expression/FunctionProvider.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(search);

class DefaultSearcherCacheStrategy : public SearcherCacheStrategy
{
public:
    DefaultSearcherCacheStrategy(autil::mem_pool::Pool *pool,
                                 bool hasSubSchema,
                                 SearchCommonResource &resource,
                                 SearchPartitionResource &searchPartitionResource);
    ~DefaultSearcherCacheStrategy();
private:
    DefaultSearcherCacheStrategy(autil::mem_pool::Pool *pool);
    DefaultSearcherCacheStrategy(const DefaultSearcherCacheStrategy &);
    DefaultSearcherCacheStrategy& operator=(const DefaultSearcherCacheStrategy &);
public:
    virtual bool init(const common::Request *request, common::MultiErrorResult* errorResult);
    virtual bool genKey(const common::Request *request, uint64_t &key);
    virtual bool beforePut(uint32_t curTime, CacheResult *cacheResult);
    virtual uint32_t filterCacheResult(CacheResult *cacheResult);
    virtual uint32_t getCacheDocNum(const common::Request *request,
                                    uint32_t docFound);
private:
    bool initAttributeExpressionCreator(const common::Request *request);
    bool createExpireTimeExpr(const common::Request *request);
    bool createFilter(const common::Request *request);
    bool calcAndfillExpireTime(CacheResult *cacheResult, uint32_t defaultExpireTime);
    bool calcExpireTime(suez::turing::AttributeExpression *expireExpr,
                        common::MatchDocs *matchDocs,
                        uint32_t &expireTime);
    uint32_t filterCacheResult(common::MatchDocs *matchDocs);
    void releaseDeletedMatchDocs(common::MatchDocs *matchDocs);

    suez::turing::AttributeExpression* getExpireTimeExpr() const { return _expireTimeExpr;}
    Filter* getFilter() const { return _filter;}

    bool needSubDoc(const common::Request *request) const;

    matchdoc::MatchDoc allocateTmpMatchDoc(
            common::MatchDocs *matchDocs, uint32_t pos);

    friend class DefaultSearcherCacheStrategyTest;
private:
    autil::mem_pool::Pool *_pool = nullptr;
    suez::turing::AttributeExpressionCreator *_attributeExpressionCreator = nullptr;
    IE_NAMESPACE(partition)::PartitionReaderSnapshot *_partitionReaderSnapshot = nullptr;
    suez::turing::AttributeExpression* _expireTimeExpr = nullptr;
    Filter* _filter = nullptr;
    suez::turing::SuezCavaAllocator *_cavaAllocator = nullptr;
    func_expression::FunctionProvider *_functionProvider = nullptr;
    const suez::turing::FunctionInterfaceCreator *_functionCreator = nullptr;
    common::Ha3MatchDocAllocatorPtr _matchDocAllocatorPtr;
    bool _hasSubSchema;
    // for new AttributeExpressionCreator
    std::string _mainTable;
    suez::turing::TableInfoPtr _tableInfoPtr;
    suez::turing::CavaPluginManagerPtr _cavaPluginManagerPtr;
private:
    static const uint32_t DEFAULT_EXPIRE_TIME_IN_SECONDS;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(DefaultSearcherCacheStrategy);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_DEFAULTSEARCHERCACHESTRATEGY_H
