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

#include <sstream>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/queryparser/QueryExprEvaluator.h"

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

class QueryExprToStringEvaluator : public QueryExprEvaluator {
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
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch
