#ifndef ISEARCH_DEFAULTQUERYEXPREVALUATOR_H
#define ISEARCH_DEFAULTQUERYEXPREVALUATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/QueryExprEvaluator.h>
#include <ha3/queryparser/BinaryQueryExpr.h>

BEGIN_HA3_NAMESPACE(queryparser);

class DefaultQueryExprEvaluator : public QueryExprEvaluator
{
public:
    DefaultQueryExprEvaluator();
    ~DefaultQueryExprEvaluator();
public:
    void evaluateAndExpr(AndQueryExpr *andExpr);
    void evaluateOrExpr(OrQueryExpr *orExpr);
    void evaluateAndNotExpr(AndNotQueryExpr *andNotExpr);
    void evaluateRankExpr(RankQueryExpr *rankExpr);

    void evaluateWordsExpr(WordsTermExpr *wordsExpr);
    void evaluateNumberExpr(NumberTermExpr *numberExpr);
    void evaluatePhraseExpr(PhraseTermExpr *phraseExpr);
    void evaluateMultiTermExpr(MultiTermQueryExpr *multiTermExpr);
    common::Query* stealQuery();
    const common::Query* getQuery();
protected:
    void setQuery(common::Query *query);
    void evaluateBinaryQueryExpr(common::Query *resultQuery, BinaryQueryExpr* expr);
private:
    common::Query *_query;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_DEFAULTQUERYEXPREVALUATOR_H
