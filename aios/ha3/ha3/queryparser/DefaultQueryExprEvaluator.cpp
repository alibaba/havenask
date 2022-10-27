#include <typeinfo>
#include <ha3/queryparser/DefaultQueryExprEvaluator.h>
#include <build_service/analyzer/Token.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/NumberQuery.h>
#include <string>
#include <ha3/common/Term.h>
#include <ha3/common/RankQuery.h>
#include <ha3/common/AndNotQuery.h>
#include <ha3/common/MultiTermQuery.h>

using namespace std;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(queryparser);

HA3_LOG_SETUP(queryparser, DefaultQueryExprEvaluator);

DefaultQueryExprEvaluator::DefaultQueryExprEvaluator()
    :_query(NULL) {}

DefaultQueryExprEvaluator::~DefaultQueryExprEvaluator() {
    delete _query;
}

Query* DefaultQueryExprEvaluator::stealQuery() {
    Query *query = _query;
    _query = NULL;
    return query;
}

const Query* DefaultQueryExprEvaluator::getQuery() {
    return _query;
}

void DefaultQueryExprEvaluator::setQuery(Query *query) {
    if (_query) {
        delete _query;
    }
    _query = query;
}

void DefaultQueryExprEvaluator::evaluateBinaryQueryExpr(Query *resultQuery,
        BinaryQueryExpr* expr)
{
    QueryExpr *leftExpr = expr->getLeftExpr();
    QueryExpr *rightExpr = expr->getRightExpr();

    leftExpr->evaluate(this);
    QueryPtr leftQueryPtr(stealQuery());
    resultQuery->addQuery(leftQueryPtr);

    rightExpr->evaluate(this);
    QueryPtr rightQueryPtr(stealQuery());
    resultQuery->addQuery(rightQueryPtr);
}

void DefaultQueryExprEvaluator::evaluateAndExpr(AndQueryExpr *expr) {
    AndQuery *query = new AndQuery(expr->getLabel());
    evaluateBinaryQueryExpr(query, expr);
    setQuery(query);
}

void DefaultQueryExprEvaluator::evaluateOrExpr(OrQueryExpr *expr) {
    OrQuery *query = new OrQuery(expr->getLabel());
    evaluateBinaryQueryExpr(query, expr);
    setQuery(query);
}

void DefaultQueryExprEvaluator::evaluateAndNotExpr(AndNotQueryExpr *expr) {
    AndNotQuery *query = new AndNotQuery(expr->getLabel());
    evaluateBinaryQueryExpr(query, expr);
    setQuery(query);
}

void DefaultQueryExprEvaluator::evaluateRankExpr(RankQueryExpr *expr) {
    RankQuery *query = new RankQuery(expr->getLabel());
    evaluateBinaryQueryExpr(query, expr);
    setQuery(query);
}

void DefaultQueryExprEvaluator::evaluateWordsExpr(WordsTermExpr *wordsExpr) {
    TermPtr searchTerm = wordsExpr->constructSearchTerm();
    Query* query = new TermQuery(*searchTerm, wordsExpr->getLabel());
    setQuery(query);
}

void DefaultQueryExprEvaluator::evaluateNumberExpr(NumberTermExpr *numberExpr) {
    NumberTermPtr searchTerm = dynamic_pointer_cast<NumberTerm>(numberExpr->constructSearchTerm());
    Query* query = new NumberQuery(*searchTerm, numberExpr->getLabel());
    setQuery(query);
}

void DefaultQueryExprEvaluator::evaluatePhraseExpr(PhraseTermExpr *phraseExpr) {
    PhraseQuery *query = new PhraseQuery(phraseExpr->getLabel());
    TermPtr searchTerm = phraseExpr->constructSearchTerm();
    query->addTerm(searchTerm);
    setQuery(query);
}

void DefaultQueryExprEvaluator::evaluateMultiTermExpr(MultiTermQueryExpr *multiTermExpr) {
    MultiTermQuery *query = new MultiTermQuery(multiTermExpr->getLabel(), multiTermExpr->getOpExpr());
    const MultiTermQueryExpr::TermExprArray& termQueryExprs = multiTermExpr->getTermExprs();
    for (MultiTermQueryExpr::TermExprArray::const_iterator it = termQueryExprs.begin();
         it != termQueryExprs.end(); it++)
    {
        TermPtr searchTerm = (*it)->constructSearchTerm();
        query->addTerm(searchTerm);
    }
    query->setOPExpr(multiTermExpr->getOpExpr());
    query->setMinShouldMatch(multiTermExpr->getMinShouldMatch());
    setQuery(query);
}

END_HA3_NAMESPACE(queryparser);
