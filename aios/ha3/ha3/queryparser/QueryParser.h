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

#include <string>
#include <vector>

#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace queryparser {
class AtomicQueryExpr;
class ParserContext;
class QueryExpr;
}  // namespace queryparser
}  // namespace isearch

namespace isearch {
namespace queryparser {

class QueryParser
{
public:
    QueryParser(const char *defaultIndex,
                QueryOperator defaultOP = OP_AND,
                bool flag = false);
    ~QueryParser();
public:
    typedef std::vector<std::string*> TextVec;

    ParserContext* parse(const char *queryText);
    ParserContext* parseExpr(const char *queryText);

    QueryExpr* createDefaultOpExpr(QueryExpr *exprA, QueryExpr *exprB);
    QueryExpr* createAndExpr(QueryExpr *exprA, QueryExpr *exprB);
    QueryExpr* createOrExpr(QueryExpr *exprA, QueryExpr *exprB);
    QueryExpr* createAndNotExpr(QueryExpr *exprA, QueryExpr *exprB);
    QueryExpr* createRankExpr(QueryExpr *exprA, QueryExpr *exprB);
    AtomicQueryExpr *createWordsTermExpr(std::string *text);
    AtomicQueryExpr *createWordsWithIndexTermExpr(char* indexName, char* text);
    AtomicQueryExpr *createPhraseTermExpr(std::string *text);
    AtomicQueryExpr *createNumberTermExpr(std::string *leftNumber, bool leftInclusive,
            std::string *rightNumber, bool rightInclusive, ParserContext *ctx);
    AtomicQueryExpr* createOrTermExpr(AtomicQueryExpr *exprA, AtomicQueryExpr *exprB);
    AtomicQueryExpr* createAndTermExpr(AtomicQueryExpr *exprA, AtomicQueryExpr *exprB);
    AtomicQueryExpr* createMultiTermQueryExpr(AtomicQueryExpr *exprA,
            AtomicQueryExpr *exprB, QueryOperator opExpr);
    void setDefaultOP(QueryOperator defaultOP) {_defaultOP = defaultOP;}
    void evaluateQuery(ParserContext* ctx);

    std::string getDefaultIndex() const { return _defaultIndex;}
    void setCurrentIndex(const std::string &currentIndex) { _currentIndex = currentIndex; }
    std::string getCurrentIndex() const { return _currentIndex;}

private:
    bool isTerminalTermExpr(AtomicQueryExpr *expr);
    bool isMultiTermExpr(AtomicQueryExpr *expr);
    bool isSameOP(AtomicQueryExpr *expr, QueryOperator opExpr);

private:
    std::string _defaultIndex;
    std::string _currentIndex;
    QueryOperator _defaultOP;
    bool _enableMultiTermQuery;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch
