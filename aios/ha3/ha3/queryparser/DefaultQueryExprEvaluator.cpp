/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/queryparser/DefaultQueryExprEvaluator.h"

#include <cstddef>
#include <memory>
#include <vector>

#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/queryparser/AndNotQueryExpr.h"
#include "ha3/queryparser/AndQueryExpr.h"
#include "ha3/queryparser/AtomicQueryExpr.h"
#include "ha3/queryparser/BinaryQueryExpr.h"
#include "ha3/queryparser/MultiTermQueryExpr.h"
#include "ha3/queryparser/NumberTermExpr.h"
#include "ha3/queryparser/OrQueryExpr.h"
#include "ha3/queryparser/PhraseTermExpr.h"
#include "ha3/queryparser/QueryExpr.h"
#include "ha3/queryparser/RankQueryExpr.h"
#include "ha3/queryparser/WordsTermExpr.h"
#include "autil/Log.h"

using namespace std;

using namespace isearch::common;

namespace isearch {
namespace queryparser {

AUTIL_LOG_SETUP(ha3, DefaultQueryExprEvaluator);

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

} // namespace queryparser
} // namespace isearch
