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
#pragma once

#include "ha3/queryparser/QueryExprEvaluator.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class Query;
}  // namespace common
namespace queryparser {
class AndNotQueryExpr;
class AndQueryExpr;
class BinaryQueryExpr;
class MultiTermQueryExpr;
class NumberTermExpr;
class OrQueryExpr;
class PhraseTermExpr;
class RankQueryExpr;
class WordsTermExpr;
}  // namespace queryparser
}  // namespace isearch

namespace isearch {
namespace queryparser {

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
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch

