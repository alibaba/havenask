#include <ha3/queryparser/PhraseTermExpr.h>
#include <ha3/queryparser/QueryExprEvaluator.h>
#include <string>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, PhraseTermExpr);

PhraseTermExpr::PhraseTermExpr(const std::string &text) : WordsTermExpr(text) {
}

PhraseTermExpr::~PhraseTermExpr() { 
}

void PhraseTermExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluatePhraseExpr(this);
}

END_HA3_NAMESPACE(queryparser);

