#include <ha3_sdk/testlib/qrs/RequestCreator.h>

using namespace std;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, RequestCreator);

RequestCreator::RequestCreator() { 
}

RequestCreator::~RequestCreator() { 
}

RequestPtr RequestCreator::prepareRequest(const string &query, 
        const string &defaultIndexName, bool multiTermFlag)
{
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(query);
    RequestParser requestParser;
#define PREPARE_CLAUSE_HELPER(clausetype)                               \
    auto clausetype##clause = requestPtr->get##clausetype();     \
    if (clausetype##clause) {                                           \
        if (requestParser.parse##clausetype(requestPtr) == false) {     \
            return RequestPtr();                                        \
        }                                                               \
    }
    PREPARE_CLAUSE_HELPER(ConfigClause);
    PREPARE_CLAUSE_HELPER(QueryLayerClause);
    PREPARE_CLAUSE_HELPER(VirtualAttributeClause);
    PREPARE_CLAUSE_HELPER(SortClause);
    PREPARE_CLAUSE_HELPER(DistinctClause);
    PREPARE_CLAUSE_HELPER(AggregateClause);
    PREPARE_CLAUSE_HELPER(RankClause);
    PREPARE_CLAUSE_HELPER(AttributeClause);
    PREPARE_CLAUSE_HELPER(FetchSummaryClause);
    PREPARE_CLAUSE_HELPER(OptimizerClause);
    PREPARE_CLAUSE_HELPER(SearcherCacheClause);
    PREPARE_CLAUSE_HELPER(RankSortClause);
    PREPARE_CLAUSE_HELPER(KVPairClause);
    PREPARE_CLAUSE_HELPER(FinalSortClause);
    PREPARE_CLAUSE_HELPER(AnalyzerClause);
    PREPARE_CLAUSE_HELPER(LevelClause);
    PREPARE_CLAUSE_HELPER(AuxQueryClause);
#undef PREPARE_CLAUSE_HELPER
    FilterClause *filterClause = requestPtr->getFilterClause();
    if (filterClause) {
        if (requestParser.parseFilterClause(filterClause) == false) {
            return RequestPtr();
        }
    }

    QueryInfo queryInfo(defaultIndexName, OP_AND, multiTermFlag);
    QueryClause *queryClause = requestPtr->getQueryClause();
    if (queryClause) {
        if (requestParser.parseQueryClause(requestPtr, queryInfo) == false) {
            return RequestPtr();
        }
    }

    AuxFilterClause *auxFilterClause = requestPtr->getAuxFilterClause();
    if (auxFilterClause) {
        if (requestParser.parseAuxFilterClause(auxFilterClause) == false) {
            return RequestPtr();
        }
    }
    
    return requestPtr;
}

END_HA3_NAMESPACE(qrs);

