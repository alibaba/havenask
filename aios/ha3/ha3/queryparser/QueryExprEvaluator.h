#ifndef ISEARCH_QUERYEXPREVALUATOR_H_
#define ISEARCH_QUERYEXPREVALUATOR_H_

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

#include <ha3/queryparser/AndNotQueryExpr.h>
#include <ha3/queryparser/OrQueryExpr.h>
#include <ha3/queryparser/RankQueryExpr.h>
#include <ha3/queryparser/AndQueryExpr.h>
#include <ha3/queryparser/PhraseTermExpr.h>
#include <ha3/queryparser/WordsTermExpr.h>
#include <ha3/queryparser/AtomicQueryExpr.h>
#include <ha3/queryparser/NumberTermExpr.h>
#include <ha3/queryparser/MultiTermQueryExpr.h>
#include <ha3/queryparser/QueryExpr.h>


BEGIN_HA3_NAMESPACE(queryparser);

class QueryExprEvaluator
{
public:
    QueryExprEvaluator() {}
    virtual ~QueryExprEvaluator() {}
public:
    virtual void evaluateAndExpr(AndQueryExpr *andExpr) = 0;
    virtual void evaluateOrExpr(OrQueryExpr *orExpr) = 0;
    virtual void evaluateAndNotExpr(AndNotQueryExpr *andNotExpr) = 0;
    virtual void evaluateRankExpr(RankQueryExpr *rankExpr) = 0;

    virtual void evaluateWordsExpr(WordsTermExpr *wordsExpr) = 0;
    virtual void evaluateNumberExpr(NumberTermExpr *numberExpr) = 0;
    virtual void evaluatePhraseExpr(PhraseTermExpr *phraseExpr) = 0;
    virtual void evaluateMultiTermExpr(MultiTermQueryExpr *multiTermExpr) = 0;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);
#endif
