#include <ha3/qrs/QueryFlatten.h>
#include <ha3/common/Query.h>

using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, QueryFlatten);

QueryFlatten::QueryFlatten() { 
    _visitedQuery = NULL;
}

QueryFlatten::~QueryFlatten() { 
    DELETE_AND_SET_NULL(_visitedQuery);
}

void QueryFlatten::visitTermQuery(const TermQuery *query){
    assert(!_visitedQuery);
    _visitedQuery = new TermQuery(*query);
}

void QueryFlatten::visitPhraseQuery(const PhraseQuery *query){
    assert(!_visitedQuery);
    _visitedQuery = new PhraseQuery(*query);
}

void QueryFlatten::visitMultiTermQuery(const MultiTermQuery *query) {
    assert(!_visitedQuery);
    _visitedQuery = new MultiTermQuery(*query);
}

void QueryFlatten::flatten(Query *resultQuery, 
                           const Query *srcQuery)
{
    const vector<QueryPtr> *childrenQuery = srcQuery->getChildQuery();
    assert(childrenQuery->size() > 1);

    vector<QueryPtr>::const_iterator it = childrenQuery->begin();
    const QueryPtr &leftChild = *it;
    if (typeid(*leftChild) == typeid(*srcQuery)
        && MDL_SUB_QUERY != leftChild->getMatchDataLevel()) {
        flatten(resultQuery, leftChild.get());
    } else {
        leftChild->accept(this);
        QueryPtr visitedQueryPtr(stealQuery());
        resultQuery->addQuery(visitedQueryPtr);
    }

    bool bFlattenRightChild = typeid(*srcQuery) == typeid(AndQuery)
                              || typeid(*srcQuery) == typeid(OrQuery);
    for(it++; it != childrenQuery->end(); it++) 
    {
        if (bFlattenRightChild && typeid(*srcQuery) == typeid(*(*it))
            && MDL_SUB_QUERY != (*it)->getMatchDataLevel()) {
            flatten(resultQuery, (*it).get());
        } else {
            (*it)->accept(this);
            QueryPtr visitedQueryPtr(stealQuery());
            resultQuery->addQuery(visitedQueryPtr);
        }
    }

}

void QueryFlatten::visitAndQuery(const AndQuery *query){
    AndQuery *resultQuery = new AndQuery(query->getQueryLabel());
    flatten(resultQuery, query);
    _visitedQuery = resultQuery;
}

void QueryFlatten::visitOrQuery(const OrQuery *query){
    OrQuery *resultQuery = new OrQuery(query->getQueryLabel());
    flatten(resultQuery, query);
    _visitedQuery = resultQuery;
}

void QueryFlatten::visitAndNotQuery(const AndNotQuery *query){
    AndNotQuery *resultQuery = new AndNotQuery(query->getQueryLabel());
    flatten(resultQuery, query);
    _visitedQuery = resultQuery;
}

void QueryFlatten::visitRankQuery(const RankQuery *query){
    RankQuery *resultQuery = new RankQuery(query->getQueryLabel());
    flatten(resultQuery, query);
    _visitedQuery = resultQuery;
}

void QueryFlatten::visitNumberQuery(const NumberQuery *query){
    assert(!_visitedQuery);
    _visitedQuery = new NumberQuery(*query);
}

Query* QueryFlatten::stealQuery() {
    Query *tmpQuery = _visitedQuery;
    _visitedQuery = NULL;
    return tmpQuery;
}

END_HA3_NAMESPACE(qrs);

