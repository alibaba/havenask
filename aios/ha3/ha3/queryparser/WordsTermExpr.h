#ifndef ISEARCH_WORDSTERMEXPR_H
#define ISEARCH_WORDSTERMEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/AtomicQueryExpr.h>
#include <string>

BEGIN_HA3_NAMESPACE(queryparser);

class WordsTermExpr : public AtomicQueryExpr
{
public:
    WordsTermExpr(const std::string &text);
    ~WordsTermExpr();
public:
    void evaluate(QueryExprEvaluator *qee);

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_WORDSTERMEXPR_H
