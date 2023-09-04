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

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace queryparser {
class AndNotQueryExpr;
class AndQueryExpr;
class MultiTermQueryExpr;
class NumberTermExpr;
class OrQueryExpr;
class PhraseTermExpr;
class RankQueryExpr;
class WordsTermExpr;
} // namespace queryparser
} // namespace isearch

namespace isearch {
namespace queryparser {

class QueryExprEvaluator {
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
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch
