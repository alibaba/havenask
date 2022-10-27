#include <ha3/search/AuxiliaryChainVisitor.h>
#include <ha3/search/TermDFVisitor.h>

using namespace std;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, AuxiliaryChainVisitor);

AuxiliaryChainVisitor::AuxiliaryChainVisitor(
        const std::string &auxChainName,
        const TermDFMap &termDFMap,
        SelectAuxChainType type)
    : _auxChainName(auxChainName)
    , _termDFMap(termDFMap)
    , _type(type)
{
}

AuxiliaryChainVisitor::~AuxiliaryChainVisitor() { 
}

void AuxiliaryChainVisitor::visitTermQuery(TermQuery *query) {
    common::Term &term = query->getTerm();
    term.setTruncateName(_auxChainName);
}

void AuxiliaryChainVisitor::visitPhraseQuery(PhraseQuery *query) {
    PhraseQuery::TermArray& terms = query->getTermArray();
    for (size_t i = 0; i < terms.size(); ++i) {
        terms[i]->setTruncateName(_auxChainName);
    }
}

void AuxiliaryChainVisitor::visitMultiTermQuery(MultiTermQuery *query) {
    MultiTermQuery::TermArray& terms = query->getTermArray();
    QueryOperator op =  query->getOpExpr();
    if(op == OP_AND && _type != SAC_ALL) {
        int32_t maxIdx = -1;
        int32_t minIdx = -1;
        df_t maxDF = 0;
        df_t minDF = numeric_limits<df_t>::max();
        for (int32_t i = 0; i < (int32_t)terms.size(); ++i) {
            TermDFMap::const_iterator iter = _termDFMap.find(*terms[i]);
            df_t df = (iter != _termDFMap.end()) ? iter->second : 0;
            if (df > maxDF) {
                maxIdx = i;
                maxDF = df;
            }
            if (df < minDF) {
                minIdx = i;
                minDF = df;
            }
        }
        if (maxIdx >= 0 && _type == SAC_DF_BIGGER) {
            terms[maxIdx]->setTruncateName(_auxChainName);
        } else if (minIdx >= 0 && _type == SAC_DF_SMALLER) {
            terms[minIdx]->setTruncateName(_auxChainName);
        }
    } else { //OP_OR, OP_WEAKAND
        for (size_t i = 0; i < terms.size(); ++i) {
            terms[i]->setTruncateName(_auxChainName);
        }
    }
}

void AuxiliaryChainVisitor::visitAndQuery(AndQuery *query) {
    vector<QueryPtr>* childQuerys = query->getChildQuery();
    if (_type == SAC_ALL) {
        for (int32_t i = 0; i < (int32_t)childQuerys->size(); ++i) {
            (*childQuerys)[i]->accept(this);
        }
        return;
    }
    int32_t maxIdx = -1;
    int32_t minIdx = -1;
    df_t maxDF = 0;
    df_t minDF = numeric_limits<df_t>::max();
    for (int32_t i = 0; i < (int32_t)childQuerys->size(); ++i) {
        TermDFVisitor dfVistor(_termDFMap);
        (*childQuerys)[i]->accept(&dfVistor);
        df_t df = dfVistor.getDF();
        if (df > maxDF) {
            maxIdx = i;
            maxDF = df;
        }
        if (df < minDF) {
            minIdx = i;
            minDF = df;
        }
    }
    if (maxIdx >= 0 && _type == SAC_DF_BIGGER) {
        (*childQuerys)[maxIdx]->accept(this);
    } else if (minIdx >= 0 && _type == SAC_DF_SMALLER) {
        (*childQuerys)[minIdx]->accept(this);
    }
}

void AuxiliaryChainVisitor::visitOrQuery(OrQuery *query) {
    vector<QueryPtr>* childQuerys = query->getChildQuery();
    for (size_t i = 0; i < childQuerys->size(); ++i) {
        (*childQuerys)[i]->accept(this);
    }
}

void AuxiliaryChainVisitor::visitAndNotQuery(AndNotQuery *query) {
    vector<QueryPtr>* childQuerys = query->getChildQuery();
    assert((*childQuerys)[0]);
    (*childQuerys)[0]->accept(this);
}

void AuxiliaryChainVisitor::visitRankQuery(RankQuery *query) {
    vector<QueryPtr>* childQuerys = query->getChildQuery();
    assert((*childQuerys)[0]);
    (*childQuerys)[0]->accept(this);
}

void AuxiliaryChainVisitor::visitNumberQuery(NumberQuery *query) {
    common::NumberTerm &term = query->getTerm();
    term.setTruncateName(_auxChainName);
}

END_HA3_NAMESPACE(search);

