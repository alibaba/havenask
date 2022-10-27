#ifndef ISEARCH_QUERYEXECUTORCONSTRUCTOR_H
#define ISEARCH_QUERYEXECUTORCONSTRUCTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3/common/Query.h>
#include <ha3/index/index.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/search/LayerMetas.h>
BEGIN_HA3_NAMESPACE(search);

class QueryExecutorConstructor
{
public:
    QueryExecutorConstructor();
    ~QueryExecutorConstructor();
public:
    static TermQueryExecutor* prepareTermQueryExecutor(
            autil::mem_pool::Pool *pool,
            const char *word, const char *indexName, 
            IndexPartitionReaderWrapper *indexReaderWrapper, 
            int32_t boost = DEFAULT_BOOST_VALUE);
    
    static QueryExecutor* preparePhraseQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper, 
            const char* indexName, 
            const char *str1, const char *str2, const char *str3, 
            int32_t boost = DEFAULT_BOOST_VALUE,
            bool pos1 = false, bool pos2 = false, bool pos3 = false);
    
    static QueryExecutor* preparePhraseQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper,
            const char* indexName, const std::vector<std::string>& strVec,
            const std::vector<bool>& posVec,
            int32_t boost = DEFAULT_BOOST_VALUE,
            common::TimeoutTerminator *timer = NULL,
            const LayerMeta *layerMeta = NULL);
    
    static QueryExecutor* prepareAndQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper, 
            const char* indexName, 
            const char *str1, const char *str2, const char *str3,
            int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor* prepareOrQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper, 
            const char* indexName, 
            const char *str1, const char *str2, const char *str3,
            int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor* prepareRankQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper, 
            const char* indexName, 
            const char *str1, const char *str2, const char *str3,
            int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor* prepareAndNotQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper, 
            const char* indexName, 
            const char *str1, const char *str2, const char *str3, 
            int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor* createQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *wrapper,
            common::Query *query);

    static QueryExecutor* prepareMultiTermAndQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper,
            const char* indexName, const char *str1,
            const char *str2, const char *str3, int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor* prepareMultiTermOrQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper,
            const char* indexName, const char *str1,
            const char *str2, const char *str3, int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor* prepareWeakAndQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper,
            const char* indexName, const char *str1,
            const char *str2, const char *str3,
            int32_t boost = DEFAULT_BOOST_VALUE, uint32_t minShouldMatch = 1);

private:
    static QueryExecutor* prepareQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper, 
            const char* indexName, 
            const char *str1, const char *str2, const char *str3,
            common::Query *query, 
            int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor* prepareMultiTermQueryExecutor(
            autil::mem_pool::Pool *pool,
            IndexPartitionReaderWrapper *indexReaderWrapper,
            const char* indexName, const char *str1, const char *str2, const char *str3, 
            common::Query *query, int32_t boost = DEFAULT_BOOST_VALUE, uint32_t minShouldMatch = 1);

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_QUERYEXECUTORCONSTRUCTOR_H
