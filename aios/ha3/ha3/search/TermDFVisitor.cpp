#include <ha3/search/TermDFVisitor.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, TermDFVisitor);

USE_HA3_NAMESPACE(common);
using namespace std;

TermDFVisitor::TermDFVisitor(const TermDFMap &auxListTerm)
    : _auxListTerm(auxListTerm)
    , _df(0)
{ 
}

TermDFVisitor::~TermDFVisitor() { 
}

void TermDFVisitor::visitTermQuery(const TermQuery *query) {
    const Term &term = query->getTerm();
    TermDFMap::const_iterator iter =
        _auxListTerm.find(term);
    if (iter != _auxListTerm.end()) {
        _df = iter->second;
    } else {
        _df = 0;
    }
}

void TermDFVisitor::visitPhraseQuery(const PhraseQuery *query) {
    df_t minDF = numeric_limits<df_t>::max();
    const PhraseQuery::TermArray &terms = query->getTermArray();
    for (PhraseQuery::TermArray::const_iterator it = terms.begin();
         it != terms.end(); ++it)
    {
        df_t df = 0;
        TermDFMap::const_iterator iter =
            _auxListTerm.find(**it);
        if (iter != _auxListTerm.end()) {
            df = iter->second;
        }
        minDF = min(minDF, df);
    }
    _df = minDF;
}

void TermDFVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    df_t minDF = numeric_limits<df_t>::max();
    df_t sumDf = 0;
    const MultiTermQuery::TermArray &terms = query->getTermArray();
    for (MultiTermQuery::TermArray::const_iterator it = terms.begin();
         it != terms.end(); ++it)
    {
        df_t df = 0;
        TermDFMap::const_iterator iter =
            _auxListTerm.find(**it);
        if (iter != _auxListTerm.end()) {
            df = iter->second;
        }
        minDF = min(minDF, df);
        sumDf += df;
    }
    _df = query->getOpExpr() == OP_AND ? minDF : sumDf;
}

void TermDFVisitor::visitAndQuery(const AndQuery *query) {
    df_t minDF = numeric_limits<df_t>::max();
    const vector<QueryPtr>* childQuerys = query->getChildQuery();
    for (size_t i = 0; i < childQuerys->size(); ++i) {
        (*childQuerys)[i]->accept(this);
        minDF = min(minDF, _df);
    }
    _df = minDF;
}

void TermDFVisitor::visitOrQuery(const OrQuery *query) {
    df_t sumDF = 0;
    const vector<QueryPtr>* childQuerys = query->getChildQuery();
    for (size_t i = 0; i < childQuerys->size(); ++i) {
        (*childQuerys)[i]->accept(this);
        sumDF += _df;
    }
    _df = sumDF;
}

void TermDFVisitor::visitAndNotQuery(const AndNotQuery *query) {
    const vector<QueryPtr>* childQuerys = query->getChildQuery();
    assert((*childQuerys)[0]);
    (*childQuerys)[0]->accept(this);
    // df_t df = _df;
    // for (size_t i = 1; i < childQuerys->size(); ++i) {
    //     (*childQuerys)[i]->accept(this);
    //     df -= _df;
    // }
    // _df = max(df, 0);
}

void TermDFVisitor::visitRankQuery(const RankQuery *query) {
    const vector<QueryPtr>* childQuerys = query->getChildQuery();
    assert((*childQuerys)[0]);
    (*childQuerys)[0]->accept(this);
}

void TermDFVisitor::visitNumberQuery(const NumberQuery *query) {
    const NumberTerm &term = query->getTerm();
    TermDFMap::const_iterator iter =
        _auxListTerm.find(term);
    if (iter != _auxListTerm.end()) {
        _df = iter->second;
    } else {
        _df = 0;
    }
}

END_HA3_NAMESPACE(search);

