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
#include "ha3/queryparser/QueryParser.h"

#include <assert.h>
#include <limits>
#include <sstream>
#include <stdint.h>
#include <stdlib.h>
#include <typeinfo>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/LongHashValue.h"
#include "autil/StringUtil.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/queryparser/AndNotQueryExpr.h"
#include "ha3/queryparser/AndQueryExpr.h"
#include "ha3/queryparser/AndTermQueryExpr.h"
#include "ha3/queryparser/AtomicQueryExpr.h"
#include "ha3/queryparser/BisonParser.hh"
#include "ha3/queryparser/DefaultQueryExprEvaluator.h"
#include "ha3/queryparser/MultiTermQueryExpr.h"
#include "ha3/queryparser/NumberTermExpr.h"
#include "ha3/queryparser/OrQueryExpr.h"
#include "ha3/queryparser/OrTermQueryExpr.h"
#include "ha3/queryparser/ParserContext.h"
#include "ha3/queryparser/PhraseTermExpr.h"
#include "ha3/queryparser/QueryExpr.h"
#include "ha3/queryparser/RankQueryExpr.h"
#include "ha3/queryparser/Scanner.h"
#include "ha3/queryparser/WordsTermExpr.h"

namespace isearch {
namespace common {
class Query;
} // namespace common
} // namespace isearch

using namespace std;
using namespace autil;
using namespace isearch::common;

namespace isearch {
namespace queryparser {

AUTIL_LOG_SETUP(ha3, QueryParser);

QueryParser::QueryParser(const char *defaultIndex, QueryOperator defaultOp, bool flag)
    : _defaultIndex(defaultIndex)
    , _defaultOP(defaultOp)
    , _enableMultiTermQuery(flag) {}

QueryParser::~QueryParser() {}

void QueryParser::evaluateQuery(ParserContext *ctx) {
    if (nullptr != ctx && ctx->getStatus() == ParserContext::OK) {
        const vector<QueryExpr *> &exprs = ctx->getQueryExprs();
        DefaultQueryExprEvaluator qee;
        for (size_t i = 0; i < exprs.size(); ++i) {
            assert(exprs[i]);
            AUTIL_LOG(TRACE3, "expr (%p)", exprs[i]);
            exprs[i]->evaluate(&qee);
            Query *query = qee.stealQuery();
            if (query) {
                ctx->addQuery(query);
            } else {
                ctx->setStatus(ParserContext::EVALUATE_FAILED);
                break;
            }
        }
    }
}

ParserContext *QueryParser::parse(const char *queryText) {
    ParserContext *ctx = parseExpr(queryText);
    evaluateQuery(ctx);
    return ctx;
}

ParserContext *QueryParser::parseExpr(const char *queryText) {
    istringstream iss(queryText);
    ostringstream oss;
    Scanner scanner(&iss, &oss);
    ParserContext *ctx = new ParserContext(*this);
    AUTIL_LOG(TRACE3, "context %p", ctx);
    isearch_bison::BisonParser parser(scanner, *ctx);
    if (parser.parse() != 0) {
        AUTIL_LOG(DEBUG, "%s:|%s|", ctx->getStatusMsg().c_str(), queryText);
        if (oss.str().length() > 0) {
            AUTIL_LOG(WARN, "failed to resolve  %s", oss.str().c_str());
        }
    }
    return ctx;
}

QueryExpr *QueryParser::createDefaultOpExpr(QueryExpr *exprA, QueryExpr *exprB) {
    AUTIL_LOG(TRACE2, "createDefaultOpExpr(%p, %p)", exprA, exprB);
    switch (_defaultOP) {
    case OP_AND:
        return createAndExpr(exprA, exprB);
    case OP_OR:
        return createOrExpr(exprA, exprB);
    default:
        return createAndExpr(exprA, exprB);
    }
}

QueryExpr *QueryParser::createAndExpr(QueryExpr *exprA, QueryExpr *exprB) {
    AUTIL_LOG(TRACE2, "createAndExpr(%p, %p)", exprA, exprB);
    return new AndQueryExpr(exprA, exprB);
}

QueryExpr *QueryParser::createAndNotExpr(QueryExpr *exprA, QueryExpr *exprB) {
    AUTIL_LOG(TRACE2, "createAndNotExpr(%p, %p)", exprA, exprB);
    return new AndNotQueryExpr(exprA, exprB);
}

QueryExpr *QueryParser::createOrExpr(QueryExpr *exprA, QueryExpr *exprB) {
    AUTIL_LOG(TRACE2, "createOrExpr(%p, %p)", exprA, exprB);
    return new OrQueryExpr(exprA, exprB);
}

QueryExpr *QueryParser::createRankExpr(QueryExpr *exprA, QueryExpr *exprB) {
    AUTIL_LOG(TRACE2, "createRankExpr(%p, %p)", exprA, exprB);
    return new RankQueryExpr(exprA, exprB);
}

AtomicQueryExpr *QueryParser::createOrTermExpr(AtomicQueryExpr *exprA, AtomicQueryExpr *exprB) {
    AUTIL_LOG(TRACE2, "createOrTermExpr(%p, %p)", exprA, exprB);
    return new OrTermQueryExpr(exprA, exprB);
}

// only support WordsTermExpr NumberTermExpr PhraseTermExpr
bool QueryParser::isTerminalTermExpr(AtomicQueryExpr *expr) {
    if (expr) {
        string exprTypeName = typeid(*expr).name();
        static string wordsTermExprTypeName = typeid(WordsTermExpr).name();
        static string numberTermExprTypeName = typeid(NumberTermExpr).name();
        static string phraseTermExprTypeName = typeid(PhraseTermExpr).name();
        return ((exprTypeName == wordsTermExprTypeName) || (exprTypeName == numberTermExprTypeName)
                || (exprTypeName == phraseTermExprTypeName));
    } else {
        return false;
    }
}

bool QueryParser::isMultiTermExpr(AtomicQueryExpr *expr) {
    if (expr) {
        string exprTypeName = typeid(*expr).name();
        static string multiTermExprTypeName = typeid(MultiTermQueryExpr).name();
        return exprTypeName == multiTermExprTypeName;
    } else {
        return false;
    }
}

bool QueryParser::isSameOP(AtomicQueryExpr *expr, QueryOperator opExpr) {
    return dynamic_cast<MultiTermQueryExpr *>(expr)->getOpExpr() == opExpr;
}

AtomicQueryExpr *QueryParser::createMultiTermQueryExpr(AtomicQueryExpr *exprA,
                                                       AtomicQueryExpr *exprB,
                                                       QueryOperator opExpr) {
    AUTIL_LOG(TRACE2, "createMultiTermExpr(%p, %p) %d", exprA, exprB, (int32_t)opExpr);
    if (!_enableMultiTermQuery) {
        if (opExpr == OP_AND) {
            return createAndTermExpr(exprA, exprB);
        }
        return createOrTermExpr(exprA, exprB);
    }

    if (isTerminalTermExpr(exprA) && isTerminalTermExpr(exprB)) {
        MultiTermQueryExpr *multiExpr = new MultiTermQueryExpr(opExpr);
        multiExpr->addTermQueryExpr(exprA);
        multiExpr->addTermQueryExpr(exprB);
        return multiExpr;
    } else if (isTerminalTermExpr(exprA) && isMultiTermExpr(exprB) && isSameOP(exprB, opExpr)) {
        dynamic_cast<MultiTermQueryExpr *>(exprB)->addTermQueryExpr(exprA);
        return exprB;
    } else if (isMultiTermExpr(exprA) && isTerminalTermExpr(exprB) && isSameOP(exprA, opExpr)) {
        dynamic_cast<MultiTermQueryExpr *>(exprA)->addTermQueryExpr(exprB);
        return exprA;
    } else {
        if (opExpr == OP_AND) {
            return createAndTermExpr(exprA, exprB);
        }
        return createOrTermExpr(exprA, exprB);
    }
}

AtomicQueryExpr *QueryParser::createAndTermExpr(AtomicQueryExpr *exprA, AtomicQueryExpr *exprB) {
    AUTIL_LOG(TRACE2, "createAndTermExpr(%p, %p)", exprA, exprB);
    return new AndTermQueryExpr(exprA, exprB);
}
AtomicQueryExpr *QueryParser::createWordsTermExpr(std::string *text) {
    AtomicQueryExpr *e = new WordsTermExpr(*text);
    AUTIL_LOG(TRACE2, "WordsExpr(%p):(%s)", e, text->c_str());
    delete text;
    return e;
}

AtomicQueryExpr *QueryParser::createPhraseTermExpr(std::string *text) {
    AtomicQueryExpr *e = new PhraseTermExpr(*text);
    AUTIL_LOG(TRACE2, "PhraseExpr(%p):(%s)", e, text->c_str());
    delete text;
    return e;
}

AtomicQueryExpr *QueryParser::createNumberTermExpr(
    string *leftStr, bool iLeft, string *rightStr, bool iRight, ParserContext *ctx) {
    int64_t left = std::numeric_limits<int64_t>::min();
    if (leftStr) {
        left = StringUtil::fromString<int64_t>(*leftStr);
        delete leftStr;
    }
    int64_t right = std::numeric_limits<int64_t>::max();
    if (rightStr) {
        right = StringUtil::fromString<int64_t>(*rightStr);
        delete rightStr;
    }
    if (left > right || (left == right && !(iLeft && iRight))) {
        ctx->setStatus(ParserContext::SYNTAX_ERROR);
        stringstream ss;
        ss << "Invalid Range: " << string(iLeft ? "[" : "(") << left << ", " << right
           << string(iRight ? "]" : ")") << ".";
        ctx->setStatusMsg(ss.str());
        return NULL;
    }
    AtomicQueryExpr *e = new NumberTermExpr(left, iLeft, right, iRight);
    AUTIL_LOG(TRACE2, "NumberExpr(%p):(%ld,%d,%ld,%d)", e, left, iLeft, right, iRight);
    return e;
}

AtomicQueryExpr *QueryParser::createWordsWithIndexTermExpr(char *indexName, char *text) {
    AtomicQueryExpr *e = new WordsTermExpr(text);
    AUTIL_LOG(TRACE2, "WordsExpr(%p):(%s)", e, text);
    e->setIndexName(indexName);
    free(indexName);
    free(text);
    return e;
}

} // namespace queryparser
} // namespace isearch
