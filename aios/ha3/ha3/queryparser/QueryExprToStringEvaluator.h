#ifndef ISEARCH_QUERYEXPRTOSTRINGEVALUATOR_H
#define ISEARCH_QUERYEXPRTOSTRINGEVALUATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <sstream>
#include <string>
#include <ha3/queryparser/QueryExprEvaluator.h>


BEGIN_HA3_NAMESPACE(queryparser);

class QueryExprToStringEvaluator : public QueryExprEvaluator
{
public:
    QueryExprToStringEvaluator();
    ~QueryExprToStringEvaluator();
public:
    void evaluateAndExpr(AndQueryExpr *andExpr);
    void evaluateOrExpr(OrQueryExpr *orExpr);
    void evaluateAndNotExpr(AndNotQueryExpr *andNotExpr);
    void evaluateRankExpr(RankQueryExpr *rankExpr);

    void evaluateWordsExpr(WordsTermExpr *wordsExpr);
    void evaluateNumberExpr(NumberTermExpr *numberExpr);
    void evaluatePhraseExpr(PhraseTermExpr *phraseExpr);
    void evaluateMultiTermExpr(MultiTermQueryExpr *multiTermExpr);

    std::string getString();
    std::string stealString();
    void reset();
private:
    std::stringstream _ss;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_QUERYEXPRTOSTRINGEVALUATOR_H
