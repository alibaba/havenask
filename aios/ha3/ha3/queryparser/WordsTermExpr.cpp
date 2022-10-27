#include <ha3/queryparser/WordsTermExpr.h>
#include <ha3/common/TermQuery.h>
#include <ha3/queryparser/QueryExprEvaluator.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, WordsTermExpr);

WordsTermExpr::WordsTermExpr(const std::string &text) { 
    setText(text);
}

WordsTermExpr::~WordsTermExpr() { 
}


void WordsTermExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateWordsExpr(this);
}

END_HA3_NAMESPACE(queryparser);

