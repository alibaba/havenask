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
#pragma once

#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/QueryVisitor.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/isearch.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_partition_reader.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace common {
class AndNotQuery;
class AndQuery;
class ColumnTerm;
class MultiTermQuery;
class NumberQuery;
class OrQuery;
class PhraseQuery;
class Query;
class RankQuery;
class TableQuery;
class DocIdsQuery;
class Term;
class TermQuery;
} // namespace common
namespace search {
class IndexPartitionReaderWrapper;
class LayerMeta;
struct LookupResult;
class MatchDataManager;
class MultiQueryExecutor;
class QueryExecutor;
} // namespace search
} // namespace isearch

namespace isearch {
namespace search {

class LiteTerm;
class TermQueryExecutor;
class QueryExecutorCreator : public common::QueryVisitor {
public:
    QueryExecutorCreator(MatchDataManager *matchDataManager,
                         IndexPartitionReaderWrapper *readerWrapper,
                         autil::mem_pool::Pool *pool,
                         common::TimeoutTerminator *timer = NULL,
                         const LayerMeta *layerMeta = NULL);
    virtual ~QueryExecutorCreator();

public:
    void visitTermQuery(const common::TermQuery *query);
    void visitPhraseQuery(const common::PhraseQuery *query);
    void visitAndQuery(const common::AndQuery *query);
    void visitOrQuery(const common::OrQuery *query);
    void visitAndNotQuery(const common::AndNotQuery *query);
    void visitRankQuery(const common::RankQuery *query);
    void visitNumberQuery(const common::NumberQuery *query);
    void visitMultiTermQuery(const common::MultiTermQuery *query);
    void visitTableQuery(const common::TableQuery *query);
    void visitDocIdsQuery(const common::DocIdsQuery *query);

    QueryExecutor *stealQuery();

private:
    static std::string composeTruncateName(const std::string &word, const std::string &chainName) {
        std::string truncateName = word;
        truncateName += DYNAMIC_CUT_SEPARATOR + chainName;
        return truncateName;
    }

    bool canConvertToBitmapAndQuery(const std::vector<QueryExecutor *> &queryExecutors);
    MultiQueryExecutor *createAndQueryExecutor(const std::vector<QueryExecutor *> &queryExecutors);
    MultiQueryExecutor *
    createMultiTermAndQueryExecutor(const std::vector<QueryExecutor *> &queryExecutors);
    void addUnpackQuery(TermQueryExecutor *termQuery,
                        const common::Term &term,
                        const common::Query *query);
    void addUnpackQuery(QueryExecutor *queryExecutor, const common::Query *query);
    std::vector<QueryExecutor *> colTermProcess(const common::ColumnTerm *col,
                                                const std::vector<QueryExecutor *> &termExecutors,
                                                QueryOperator op);
    QueryExecutor *mergeQueryExecutor(const std::vector<QueryExecutor *> &childQueryExecutors,
                                      QueryOperator op);
    void visitColumnTerm(const common::ColumnTerm &term, std::vector<QueryExecutor *> &results);
    void visitNumberColumnTerm(const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader,
                               bool isSubIndex,
                               const common::ColumnTerm &term,
                               std::vector<QueryExecutor *> &results);
    void visitPk64ColumnTerm(const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader,
                             bool isSubIndex,
                             const common::ColumnTerm &term,
                             std::vector<QueryExecutor *> &results);
    void visitNormalColumnTerm(const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader,
                               bool isSubIndex,
                               const common::ColumnTerm &term,
                               std::vector<QueryExecutor *> &results);
    template <class T, class Traits>
    LookupResult lookupNormalIndexWithoutCache(Traits traits,
                                               const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader,
                                               bool isSubIndex,
                                               LiteTerm &term,
                                               PostingType pt1,
                                               PostingType pt2,
                                               const T &value);
    template <class T, class Traits>
    void lookupNormalIndex(Traits traits,
                           const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader,
                           bool isSubIndex,
                           const common::ColumnTerm &term,
                           std::vector<QueryExecutor *> &results);
    template <class T, class Traits>
    void lookupPk64Index(Traits traits,
                         const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader,
                         bool isSubIndex,
                         const common::ColumnTerm &term,
                         std::vector<QueryExecutor *> &results);

public:
    static TermQueryExecutor *createTermQueryExecutor(IndexPartitionReaderWrapper *readerWrapper,
                                                      const common::Term &queryTerm,
                                                      autil::mem_pool::Pool *pool,
                                                      const LayerMeta *layerMeta);
    static TermQueryExecutor *createTermQueryExecutor(IndexPartitionReaderWrapper *readerWrapper,
                                                      const common::Term &queryTerm,
                                                      const LookupResult &result,
                                                      autil::mem_pool::Pool *pool,
                                                      const LayerMeta *layerMeta);

private:
    static TermQueryExecutor *doCreateTermQueryExecutor(IndexPartitionReaderWrapper *readerWrapper,
                                                        const common::Term &queryTerm,
                                                        const LookupResult &result,
                                                        autil::mem_pool::Pool *pool,
                                                        const LayerMeta *layerMeta);

private:
    MatchDataManager *_matchDataManager;
    QueryExecutor *_queryExecutor;
    IndexPartitionReaderWrapper *_readerWrapper;
    int32_t _andnotQueryLevel; // add left termQueryexecutor
    autil::mem_pool::Pool *_pool;
    common::TimeoutTerminator *_timer;
    const LayerMeta *_layerMeta;

private:
    friend class QueryExecutorCreatorTest;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
