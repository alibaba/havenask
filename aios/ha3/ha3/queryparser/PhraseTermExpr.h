#ifndef ISEARCH_PHRASETERMEXPR_H
#define ISEARCH_PHRASETERMEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/WordsTermExpr.h>

BEGIN_HA3_NAMESPACE(queryparser);

class PhraseTermExpr : public WordsTermExpr
{
public:
    PhraseTermExpr(const std::string &text);
    ~PhraseTermExpr();
public:
    void evaluate(QueryExprEvaluator *qee);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_PHRASETERMEXPR_H
