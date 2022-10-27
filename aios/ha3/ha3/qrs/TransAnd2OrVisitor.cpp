#include <ha3/qrs/TransAnd2OrVisitor.h>
#include <ha3/common/Query.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, TransAnd2OrVisitor);

TransAnd2OrVisitor::TransAnd2OrVisitor() {
}

TransAnd2OrVisitor::~TransAnd2OrVisitor() {
}

void TransAnd2OrVisitor::visitTermQuery(const TermQuery *query){
    _query = query->clone();
}

void TransAnd2OrVisitor::visitPhraseQuery(const PhraseQuery *query){
    const PhraseQuery::TermArray &termArray = query->getTermArray();
    Query *orQuery = new OrQuery(query->getQueryLabel());
    for (size_t i = 0; i < termArray.size(); ++i) {
        QueryPtr termQueryPtr(new TermQuery(*termArray[i], ""));
        orQuery->addQuery(termQueryPtr);
    }
    _query = orQuery;
}

void TransAnd2OrVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    MultiTermQuery *multiTermQuery = new MultiTermQuery(*query);
    multiTermQuery->setOPExpr(OP_OR);
    _query = multiTermQuery;
}

void TransAnd2OrVisitor::visitAndQuery(const AndQuery *query){
    visitAdvancedQuery(query);
}

void TransAnd2OrVisitor::visitOrQuery(const OrQuery *query){
    visitAdvancedQuery(query);
}

void TransAnd2OrVisitor::visitAndNotQuery(const AndNotQuery *query){
    visitAdvancedQuery(query);
}

void TransAnd2OrVisitor::visitRankQuery(const RankQuery *query){
    visitAdvancedQuery(query);
}

void TransAnd2OrVisitor::visitNumberQuery(const NumberQuery *query){
    _query = query->clone();
}
Query *TransAnd2OrVisitor::stealQuery(){
    Query *tmp = _query;
    _query = NULL;
    return tmp;
}
void TransAnd2OrVisitor::visitAdvancedQuery(const Query *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    OrQuery *orQuery = new OrQuery(query->getQueryLabel());
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
        QueryPtr childQueryPtr(stealQuery());
        orQuery->addQuery(childQueryPtr);
    }
    _query = orQuery;
}

END_HA3_NAMESPACE(qrs);

