#include <ha3/common/QueryTermVisitor.h>
#include <ha3/common/Query.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, QueryTermVisitor);

QueryTermVisitor::QueryTermVisitor(VisitTermType visitType) {
    _visitTermType = visitType;
}

QueryTermVisitor::~QueryTermVisitor() {
}

void QueryTermVisitor::visitTermQuery(const TermQuery *query){
    const Term &term = query->getTerm();
    _termVector.push_back(term);
}

void QueryTermVisitor::visitPhraseQuery(const PhraseQuery *query){
    const PhraseQuery::TermArray &termArray = query->getTermArray();
    for (size_t i = 0; i < termArray.size(); ++i) {
        _termVector.push_back(*termArray[i]);
    }
}

void QueryTermVisitor::visitAndQuery(const AndQuery *query){
    visitAdvancedQuery(query);
}

void QueryTermVisitor::visitOrQuery(const OrQuery *query){
    visitAdvancedQuery(query);
}

void QueryTermVisitor::visitAndNotQuery(const AndNotQuery *query){
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    assert(children->size() > 0);
    (*children)[0]->accept(this);
    if (_visitTermType & VT_ANDNOT_QUERY) {
        for (size_t i = 1; i < children->size(); ++i) {
            (*children)[i]->accept(this);
        }
    }
}

void QueryTermVisitor::visitRankQuery(const RankQuery *query){
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    assert(children->size() > 0);
    (*children)[0]->accept(this);
    if (_visitTermType & VT_RANK_QUERY) {
        for (size_t i = 1; i < children->size(); ++i) {
            (*children)[i]->accept(this);
        }
    }
}

void QueryTermVisitor::visitNumberQuery(const NumberQuery *query){
    ;
}

void QueryTermVisitor::visitAdvancedQuery(const Query *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
    }
}

void QueryTermVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    const MultiTermQuery::TermArray &termArray = query->getTermArray();
    for (size_t i = 0; i < termArray.size(); ++i) {
        _termVector.push_back(*termArray[i]);
    }
}

END_HA3_NAMESPACE(common);

