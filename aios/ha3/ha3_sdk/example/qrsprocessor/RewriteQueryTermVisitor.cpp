#include "RewriteQueryTermVisitor.h"
#include <ha3/common/Query.h>
using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, RewriteQueryTermVisitor);

RewriteQueryTermVisitor::RewriteQueryTermVisitor() {
}

RewriteQueryTermVisitor::
RewriteQueryTermVisitor(const TokenRewriteMap &tokenRewriteMap)
{
    _tokenRewriteMap = tokenRewriteMap;
}

RewriteQueryTermVisitor::~RewriteQueryTermVisitor() {
}

void RewriteQueryTermVisitor::rewriteTerm(Term &term){
    string word = string(term.getWord().c_str());
    HA3_LOG(DEBUG, "RewriteQueryTermVisitor::visitTermQuery query word:[%s]", word.c_str());
    TokenRewriteMap::iterator rewriteIt = _tokenRewriteMap.find(word);
    if (rewriteIt != _tokenRewriteMap.end()) {
        term.setWord((rewriteIt->second).c_str());
        HA3_LOG(DEBUG, "RewriteQueryTermVisitor::visitTermQuery rewrite:[%s] by [%s](%s)", 
            word.c_str(), term.getWord().c_str(), rewriteIt->second.c_str());
    }
}

void RewriteQueryTermVisitor::visitTermQuery(const TermQuery *query){
    const Term &term = query->getTerm();
    Term &tmpTerm = const_cast<Term&>(term);
    rewriteTerm(tmpTerm);
}

void RewriteQueryTermVisitor::visitPhraseQuery(const PhraseQuery *query){
    const PhraseQuery::TermArray &termArray = query->getTermArray();
    PhraseQuery::TermArray &tmpArray = const_cast<PhraseQuery::TermArray&>(termArray);
    for (size_t i = 0; i < tmpArray.size(); ++i) {
        rewriteTerm(*tmpArray[i]);
    }
}

void RewriteQueryTermVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    const MultiTermQuery::TermArray &termArray = query->getTermArray();
    MultiTermQuery::TermArray &tmpArray = const_cast<MultiTermQuery::TermArray&>(termArray);
    for (size_t i = 0; i < tmpArray.size(); ++i) {
        rewriteTerm(*tmpArray[i]);
    }
}

void RewriteQueryTermVisitor::visitAndQuery(const AndQuery *query){
    visitAdvancedQuery(query);
}

void RewriteQueryTermVisitor::visitOrQuery(const OrQuery *query){
    visitAdvancedQuery(query);
}

void RewriteQueryTermVisitor::visitAndNotQuery(const AndNotQuery *query){
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    assert(children->size() > 0);
    (*children)[0]->accept(this);
}

void RewriteQueryTermVisitor::visitRankQuery(const RankQuery *query){
    visitAdvancedQuery(query);
}

void RewriteQueryTermVisitor::visitNumberQuery(const NumberQuery *query){
    ;
}

void RewriteQueryTermVisitor::visitAdvancedQuery(const Query *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    if (!children) {
        return;
    }
    assert(children);
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
    }
}

END_HA3_NAMESPACE(common);

