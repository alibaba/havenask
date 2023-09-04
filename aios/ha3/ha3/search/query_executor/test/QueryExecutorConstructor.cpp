#include "ha3/search/test/QueryExecutorConstructor.h"

#include <iosfwd>
#include <memory>

#include "autil/Log.h"
#include "build_service/analyzer/Token.h"
#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/isearch.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/QueryExecutorCreator.h"
#include "ha3/search/TermQueryExecutor.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace search {
class IndexPartitionReaderWrapper;
class LayerMeta;
} // namespace search
} // namespace isearch

using namespace isearch::common;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, QueryExecutorConstructor);
using namespace std;

QueryExecutorConstructor::QueryExecutorConstructor() {}

QueryExecutorConstructor::~QueryExecutorConstructor() {}

TermQueryExecutor *
QueryExecutorConstructor::prepareTermQueryExecutor(autil::mem_pool::Pool *pool,
                                                   const char *word,
                                                   const char *indexName,
                                                   IndexPartitionReaderWrapper *indexReaderWrapper,
                                                   int32_t boost) {
    RequiredFields requiredFields;
    Term term(word, indexName, requiredFields, boost);
    TermQuery termQuery(term, "");
    MatchDataManager manager;
    QueryExecutorCreator qeCreator(&manager, indexReaderWrapper, pool);
    termQuery.accept(&qeCreator);
    QueryExecutor *queryExecutor = qeCreator.stealQuery();
    return dynamic_cast<TermQueryExecutor *>(queryExecutor);
}

QueryExecutor *QueryExecutorConstructor::preparePhraseQueryExecutor(
    autil::mem_pool::Pool *pool,
    IndexPartitionReaderWrapper *indexReaderWrapper,
    const char *indexName,
    const char *str1,
    const char *str2,
    const char *str3,
    int32_t boost,
    bool stop1,
    bool stop2,
    bool stop3) {
    PhraseQuery phraseQuery("");
    RequiredFields requiredFields;
    if (str1) {
        TermPtr term1(new Term(str1, indexName, requiredFields, boost));
        term1->getToken().setIsStopWord(stop1);
        phraseQuery.addTerm(term1);
    }
    if (str2) {
        TermPtr term2(new Term(str2, indexName, requiredFields, boost));
        term2->getToken().setIsStopWord(stop2);
        phraseQuery.addTerm(term2);
    }
    if (str3) {
        TermPtr term3(new Term(str3, indexName, requiredFields, boost));
        term3->getToken().setIsStopWord(stop3);
        phraseQuery.addTerm(term3);
    }

    MatchDataManager manager;
    QueryExecutorCreator qeCreator(&manager, indexReaderWrapper, pool);
    phraseQuery.accept(&qeCreator);
    QueryExecutor *queryExecutor = qeCreator.stealQuery();
    return queryExecutor;
}

QueryExecutor *
QueryExecutorConstructor::prepareAndQueryExecutor(autil::mem_pool::Pool *pool,
                                                  IndexPartitionReaderWrapper *indexReaderWrapper,
                                                  const char *indexName,
                                                  const char *str1,
                                                  const char *str2,
                                                  const char *str3,
                                                  int32_t boost) {
    AndQuery andQuery("");
    return prepareQueryExecutor(
        pool, indexReaderWrapper, indexName, str1, str2, str3, &andQuery, boost);
}

QueryExecutor *
QueryExecutorConstructor::prepareOrQueryExecutor(autil::mem_pool::Pool *pool,
                                                 IndexPartitionReaderWrapper *indexReaderWrapper,
                                                 const char *indexName,
                                                 const char *str1,
                                                 const char *str2,
                                                 const char *str3,
                                                 int32_t boost) {
    OrQuery orQuery("");
    return prepareQueryExecutor(
        pool, indexReaderWrapper, indexName, str1, str2, str3, &orQuery, boost);
}

QueryExecutor *
QueryExecutorConstructor::prepareRankQueryExecutor(autil::mem_pool::Pool *pool,
                                                   IndexPartitionReaderWrapper *indexReaderWrapper,
                                                   const char *indexName,
                                                   const char *str1,
                                                   const char *str2,
                                                   const char *str3,
                                                   int32_t boost) {
    RankQuery rankQuery("");
    return prepareQueryExecutor(
        pool, indexReaderWrapper, indexName, str1, str2, str3, &rankQuery, boost);
}

QueryExecutor *QueryExecutorConstructor::prepareAndNotQueryExecutor(
    autil::mem_pool::Pool *pool,
    IndexPartitionReaderWrapper *indexReaderWrapper,
    const char *indexName,
    const char *str1,
    const char *str2,
    const char *str3,
    int32_t boost) {
    AndNotQuery andNotQuery("");
    return prepareQueryExecutor(
        pool, indexReaderWrapper, indexName, str1, str2, str3, &andNotQuery, boost);
}

QueryExecutor *QueryExecutorConstructor::prepareMultiTermQueryExecutor(
    autil::mem_pool::Pool *pool,
    IndexPartitionReaderWrapper *indexReaderWrapper,
    const char *indexName,
    const char *str1,
    const char *str2,
    const char *str3,
    Query *query,
    int32_t boost,
    uint32_t minShouldMatch) {
    MultiTermQuery *multiTermQuery = static_cast<MultiTermQuery *>(query);
    RequiredFields requiredFields;
    if (str1) {
        TermPtr term(new Term(str1, indexName, requiredFields, boost));
        multiTermQuery->addTerm(term);
    }
    if (str2) {
        TermPtr term(new Term(str2, indexName, requiredFields, boost));
        multiTermQuery->addTerm(term);
    }
    if (str3) {
        TermPtr term(new Term(str3, indexName, requiredFields, boost));
        multiTermQuery->addTerm(term);
    }
    multiTermQuery->setMinShouldMatch(minShouldMatch);
    MatchDataManager manager;
    QueryExecutorCreator qeCreator(&manager, indexReaderWrapper, pool);
    multiTermQuery->accept(&qeCreator);
    return qeCreator.stealQuery();
}

QueryExecutor *
QueryExecutorConstructor::prepareQueryExecutor(autil::mem_pool::Pool *pool,
                                               IndexPartitionReaderWrapper *indexReaderWrapper,
                                               const char *indexName,
                                               const char *str1,
                                               const char *str2,
                                               const char *str3,
                                               Query *query,
                                               int32_t boost) {
    RequiredFields requiredFields;
    if (str1) {
        Term term1(str1, indexName, requiredFields, boost);
        QueryPtr termQueryPtr1(new TermQuery(term1, ""));
        query->addQuery(termQueryPtr1);
    }
    if (str2) {
        Term term2(str2, indexName, requiredFields, boost);
        QueryPtr termQueryPtr2(new TermQuery(term2, ""));
        query->addQuery(termQueryPtr2);
    }
    if (str3) {
        Term term3(str3, indexName, requiredFields, boost);
        QueryPtr termQueryPtr3(new TermQuery(term3, ""));
        query->addQuery(termQueryPtr3);
    }
    MatchDataManager manager;
    QueryExecutorCreator qeCreator(&manager, indexReaderWrapper, pool);
    query->accept(&qeCreator);
    QueryExecutor *queryExecutor = qeCreator.stealQuery();

    return queryExecutor;
}

QueryExecutor *QueryExecutorConstructor::preparePhraseQueryExecutor(
    autil::mem_pool::Pool *pool,
    IndexPartitionReaderWrapper *indexReaderWrapper,
    const char *indexName,
    const vector<string> &strVec,
    const vector<bool> &posVec,
    int32_t boost,
    common::TimeoutTerminator *timer,
    const LayerMeta *layerMeta) {
    PhraseQuery phrasequery("");
    RequiredFields requiredFields;
    for (uint32_t i = 0; i < strVec.size(); i++) {
        TermPtr term(new Term(strVec[i].c_str(), indexName, requiredFields, boost));
        if (i < posVec.size())
            term->getToken().setIsStopWord(posVec[i]);
        phrasequery.addTerm(term);
    }
    MatchDataManager manager;
    QueryExecutorCreator qeCreator(&manager, indexReaderWrapper, pool, timer, layerMeta);
    phrasequery.accept(&qeCreator);
    QueryExecutor *queryExecutor = qeCreator.stealQuery();

    return queryExecutor;
}

QueryExecutor *QueryExecutorConstructor::prepareMultiTermAndQueryExecutor(
    autil::mem_pool::Pool *pool,
    IndexPartitionReaderWrapper *indexReaderWrapper,
    const char *indexName,
    const char *str1,
    const char *str2,
    const char *str3,
    int32_t boost) {
    MultiTermQuery multiTermQuery("");
    multiTermQuery.setOPExpr(OP_AND);
    return prepareMultiTermQueryExecutor(
        pool, indexReaderWrapper, indexName, str1, str2, str3, (Query *)&multiTermQuery, boost);
}

QueryExecutor *QueryExecutorConstructor::prepareMultiTermOrQueryExecutor(
    autil::mem_pool::Pool *pool,
    IndexPartitionReaderWrapper *indexReaderWrapper,
    const char *indexName,
    const char *str1,
    const char *str2,
    const char *str3,
    int32_t boost) {
    MultiTermQuery multiTermQuery("");
    multiTermQuery.setOPExpr(OP_OR);
    return prepareMultiTermQueryExecutor(
        pool, indexReaderWrapper, indexName, str1, str2, str3, (Query *)&multiTermQuery, boost);
}

QueryExecutor *QueryExecutorConstructor::prepareWeakAndQueryExecutor(
    autil::mem_pool::Pool *pool,
    IndexPartitionReaderWrapper *indexReaderWrapper,
    const char *indexName,
    const char *str1,
    const char *str2,
    const char *str3,
    int32_t boost,
    uint32_t minShouldMatch) {
    MultiTermQuery multiTermQuery("");
    multiTermQuery.setOPExpr(OP_WEAKAND);
    return prepareMultiTermQueryExecutor(pool,
                                         indexReaderWrapper,
                                         indexName,
                                         str1,
                                         str2,
                                         str3,
                                         (Query *)&multiTermQuery,
                                         boost,
                                         minShouldMatch);
}

QueryExecutor *QueryExecutorConstructor::createQueryExecutor(autil::mem_pool::Pool *pool,
                                                             IndexPartitionReaderWrapper *wrapper,
                                                             Query *query) {
    MatchDataManager manager;
    QueryExecutorCreator qeCreator(&manager, wrapper, pool);
    query->accept(&qeCreator);
    QueryExecutor *queryExecutor = qeCreator.stealQuery();
    return queryExecutor;
}

} // namespace search
} // namespace isearch
