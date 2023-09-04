#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/isearch.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace common {
class Query;
} // namespace common
namespace search {
class IndexPartitionReaderWrapper;
class LayerMeta;
class QueryExecutor;
class TermQueryExecutor;
} // namespace search
} // namespace isearch

namespace isearch {
namespace search {

class QueryExecutorConstructor {
public:
    QueryExecutorConstructor();
    ~QueryExecutorConstructor();

public:
    static TermQueryExecutor *
    prepareTermQueryExecutor(autil::mem_pool::Pool *pool,
                             const char *word,
                             const char *indexName,
                             IndexPartitionReaderWrapper *indexReaderWrapper,
                             int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor *
    preparePhraseQueryExecutor(autil::mem_pool::Pool *pool,
                               IndexPartitionReaderWrapper *indexReaderWrapper,
                               const char *indexName,
                               const char *str1,
                               const char *str2,
                               const char *str3,
                               int32_t boost = DEFAULT_BOOST_VALUE,
                               bool pos1 = false,
                               bool pos2 = false,
                               bool pos3 = false);

    static QueryExecutor *
    preparePhraseQueryExecutor(autil::mem_pool::Pool *pool,
                               IndexPartitionReaderWrapper *indexReaderWrapper,
                               const char *indexName,
                               const std::vector<std::string> &strVec,
                               const std::vector<bool> &posVec,
                               int32_t boost = DEFAULT_BOOST_VALUE,
                               common::TimeoutTerminator *timer = NULL,
                               const LayerMeta *layerMeta = NULL);

    static QueryExecutor *prepareAndQueryExecutor(autil::mem_pool::Pool *pool,
                                                  IndexPartitionReaderWrapper *indexReaderWrapper,
                                                  const char *indexName,
                                                  const char *str1,
                                                  const char *str2,
                                                  const char *str3,
                                                  int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor *prepareOrQueryExecutor(autil::mem_pool::Pool *pool,
                                                 IndexPartitionReaderWrapper *indexReaderWrapper,
                                                 const char *indexName,
                                                 const char *str1,
                                                 const char *str2,
                                                 const char *str3,
                                                 int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor *prepareRankQueryExecutor(autil::mem_pool::Pool *pool,
                                                   IndexPartitionReaderWrapper *indexReaderWrapper,
                                                   const char *indexName,
                                                   const char *str1,
                                                   const char *str2,
                                                   const char *str3,
                                                   int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor *
    prepareAndNotQueryExecutor(autil::mem_pool::Pool *pool,
                               IndexPartitionReaderWrapper *indexReaderWrapper,
                               const char *indexName,
                               const char *str1,
                               const char *str2,
                               const char *str3,
                               int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor *createQueryExecutor(autil::mem_pool::Pool *pool,
                                              IndexPartitionReaderWrapper *wrapper,
                                              common::Query *query);

    static QueryExecutor *
    prepareMultiTermAndQueryExecutor(autil::mem_pool::Pool *pool,
                                     IndexPartitionReaderWrapper *indexReaderWrapper,
                                     const char *indexName,
                                     const char *str1,
                                     const char *str2,
                                     const char *str3,
                                     int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor *
    prepareMultiTermOrQueryExecutor(autil::mem_pool::Pool *pool,
                                    IndexPartitionReaderWrapper *indexReaderWrapper,
                                    const char *indexName,
                                    const char *str1,
                                    const char *str2,
                                    const char *str3,
                                    int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor *
    prepareWeakAndQueryExecutor(autil::mem_pool::Pool *pool,
                                IndexPartitionReaderWrapper *indexReaderWrapper,
                                const char *indexName,
                                const char *str1,
                                const char *str2,
                                const char *str3,
                                int32_t boost = DEFAULT_BOOST_VALUE,
                                uint32_t minShouldMatch = 1);

private:
    static QueryExecutor *prepareQueryExecutor(autil::mem_pool::Pool *pool,
                                               IndexPartitionReaderWrapper *indexReaderWrapper,
                                               const char *indexName,
                                               const char *str1,
                                               const char *str2,
                                               const char *str3,
                                               common::Query *query,
                                               int32_t boost = DEFAULT_BOOST_VALUE);

    static QueryExecutor *
    prepareMultiTermQueryExecutor(autil::mem_pool::Pool *pool,
                                  IndexPartitionReaderWrapper *indexReaderWrapper,
                                  const char *indexName,
                                  const char *str1,
                                  const char *str2,
                                  const char *str3,
                                  common::Query *query,
                                  int32_t boost = DEFAULT_BOOST_VALUE,
                                  uint32_t minShouldMatch = 1);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
