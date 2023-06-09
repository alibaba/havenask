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
#include "ha3/common/QueryTokenizer.h"

#include <stdint.h>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "indexlib/analyzer/Analyzer.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/analyzer/IdleTokenizer.h"
#include "build_service/analyzer/Token.h"
#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"
#include "indexlib/indexlib.h"
#include "suez/turing/expression/util/IndexInfos.h"

namespace isearch {
namespace common {
class NumberTerm;
}  // namespace common
}  // namespace isearch

using namespace std;
using namespace suez::turing;
using namespace build_service::analyzer;
using namespace indexlibv2::analyzer;
using namespace isearch::common;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, QueryTokenizer);

QueryTokenizer::QueryTokenizer(const AnalyzerFactory *analyzerFactory)
    : _indexInfos(nullptr), _defaultOP(OP_AND), _analyzerFactory(analyzerFactory),
      _visitedQuery(nullptr), _needCleanStopWords(false)
{
}

QueryTokenizer::QueryTokenizer(const QueryTokenizer &tokenzier) {
    _indexInfos = tokenzier._indexInfos;
    _analyzerFactory = tokenzier._analyzerFactory;
    _visitedQuery = nullptr;
    _defaultOP = OP_AND;
    _noTokenizeIndexes = tokenzier._noTokenizeIndexes;
    _globalAnalyzerName = tokenzier._globalAnalyzerName;
    _indexAnalyzerMap = tokenzier._indexAnalyzerMap;
    _needCleanStopWords = tokenzier._needCleanStopWords;
}

QueryTokenizer::~QueryTokenizer() {
    DELETE_AND_SET_NULL(_visitedQuery);
}

void QueryTokenizer::tokenize(const string &indexName,
                              const Term &term,
                              TokenVect &tokens,
                              bool needCleanStopWords)
{
    if (needTokenize(indexName)) {
        doTokenize(indexName, term.getWord(), tokens, needCleanStopWords);
    } else {
        if (needCleanStopWords && term.getToken().isStopWord()) {
            return;
        }
        tokens.push_back(Token(term.getWord(), 0));
    }
}

bool QueryTokenizer::needTokenize(const string &indexName) {
    assert(_indexInfos);
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    if (nullptr == indexInfo) {
        _errorResult.resetError(ERROR_INDEX_NOT_EXIST,
                                string("Index name:") + indexName);
        return false;
    }

    InvertedIndexType indexType = indexInfo->getIndexType();
    return (it_text == indexType) || (it_pack == indexType)
        || (it_expack == indexType);
}

void QueryTokenizer::doTokenize(const string &indexName,
                              const string &word,
                                TokenVect &tokens,
                                bool needCleanStopWords)
{
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    string analyzerName = getIndexAnalyzerName(indexName);
    Analyzer* analyzer = nullptr;
    if (analyzerName.empty()) {
        analyzerName = indexInfo->getAnalyzerName();
    }
    analyzer = _analyzerFactory->createAnalyzer(analyzerName);
    if (!analyzer){
        _errorResult.resetError(ERROR_ANALYZER_NOT_EXIST,
                                string("Analyzer name: ") + analyzerName);
        return;
    }
    bool needIdleTokenize = isIndexNoTokenize(indexName);
    if (needIdleTokenize) {
        analyzer->resetTokenizer(new IdleTokenizer());
    }
    analyzer->tokenize(word.data(), word.size());
    Token token;
    while (analyzer->next(token)) {
        if (!token.isSpace()
            && !token.isDelimiter()
            && token.isRetrieve())
        {
            if (needCleanStopWords && token.isStopWord()) {
                continue;
            }
            tokens.push_back(token);
        }
    }
    DELETE_AND_SET_NULL(analyzer);
}

void QueryTokenizer::visitTermQuery(const TermQuery *query) {
    const Term &term = query->getTerm();
    const string indexName = term.getIndexName();
    TokenVect tokens;
    tokenize(indexName, term, tokens, _needCleanStopWords);
    uint32_t tokenNum = tokens.size();
    if (tokenNum == 1) {
        _visitedQuery = createTermQuery(tokens[0], term, query->getQueryLabel());
    } else if (tokenNum > 1) {
        _visitedQuery = createDefaultQuery(tokens, term, query->getQueryLabel());
    }
}

void QueryTokenizer::visitPhraseQuery(const PhraseQuery *query) {
    const Query::TermArray &termArray = query->getTermArray();
    assert(termArray.size() == 1);
    TermPtr term = termArray[0];
    const string indexName = term->getIndexName();
    TokenVect tokens;
    tokenize(indexName, *term, tokens, false);
    uint32_t tokenNum = tokens.size();
    if (tokenNum == 1) {
        _visitedQuery = createTermQuery(tokens[0], *term, query->getQueryLabel());
    } else if (tokenNum > 1) {
        PhraseQuery *phraseQuery = new PhraseQuery(query->getQueryLabel());
        addMultiTerms(phraseQuery->getTermArray(), tokens, *term);
        _visitedQuery = phraseQuery;
    }
}

void QueryTokenizer::visitMultiTermQuery(const MultiTermQuery *query) {
    const Query::TermArray &termArray = query->getTermArray();
    assert(termArray.size());

    const string indexName = termArray[0]->getIndexName();
    QueryOperator op = query->getOpExpr();
    if (op != OP_WEAKAND && needTokenize(indexName)) {
        if (op != _defaultOP) {
            reConstructBinaryQuery(query);
            return;
        }

        MultiTermQuery *multiTermQuery = new MultiTermQuery(query->getQueryLabel(), op);
        Query::TermArray &resultTermArray = multiTermQuery->getTermArray() ;
        for (size_t i = 0; i < termArray.size(); ++i) {
            TokenVect tokens;
            doTokenize(indexName, termArray[i]->getWord().c_str(),
                       tokens, _needCleanStopWords);
            addMultiTerms(resultTermArray, tokens, *termArray[i]);
        }
        size_t termSize = resultTermArray.size();
        if (termSize > 1) {
            _visitedQuery = multiTermQuery;
        } else if (termSize == 1) {
            _visitedQuery = new TermQuery(*resultTermArray[0], query->getQueryLabel());
            delete multiTermQuery;
        } else {
            delete multiTermQuery;
        }
    } else {
        _visitedQuery = new MultiTermQuery(*query);
    }
}

void QueryTokenizer::reConstructBinaryQuery(const MultiTermQuery *query) {
    Query *resultQuery = nullptr;
    QueryOperator op = query->getOpExpr();
    if (op == OP_AND) {
        resultQuery =  new AndQuery(query->getQueryLabel());
    } else if (op == OP_OR) {
        resultQuery = new OrQuery(query->getQueryLabel());
    } else {
        assert(false);
    }

    const Query::TermArray &termArray = query->getTermArray();
    const string indexName = termArray[0]->getIndexName();

    MultiTermQuery *multiTermQuery = new MultiTermQuery("", query->getOpExpr());
    Query::TermArray &resultTermArray = multiTermQuery->getTermArray() ;
    for (size_t i = 0; i < termArray.size(); ++i) {
        TokenVect tokens;
        doTokenize(indexName, termArray[i]->getWord().c_str(),
                   tokens, _needCleanStopWords);
        size_t tokenSize = tokens.size();
        if (tokenSize == 1) {
            addMultiTerms(resultTermArray, tokens, *termArray[i]);
        } else if (tokenSize > 1) {
            Query *oneQuery = createDefaultQuery(tokens, *termArray[i], "");
            resultQuery->addQuery(QueryPtr(oneQuery));
        } // do nothing when token size == 0
    }

    Query *lastQuery = nullptr;
    size_t termSize = resultTermArray.size();
    if (termSize > 1) {
        lastQuery = multiTermQuery;
    } else if (termSize == 1) {
        lastQuery = new TermQuery(*resultTermArray[0], "");
        delete multiTermQuery;
    } else {
        delete multiTermQuery;
    }

    if (resultQuery->getChildQuery()->size() > 0) {
        if(nullptr != lastQuery) {
            resultQuery->addQuery(QueryPtr(lastQuery));
        }
        _visitedQuery = resultQuery;
    } else {
        if (nullptr != lastQuery) {
            lastQuery->setQueryLabelWithDefaultLevel(query->getQueryLabel());
        }
        _visitedQuery = lastQuery;
        delete resultQuery;
    }
}

void QueryTokenizer::visitNumberQuery(const NumberQuery *query) {
    const NumberTerm &numberTerm = query->getTerm();
    Query *resultQuery = new NumberQuery(numberTerm, query->getQueryLabel());
    _visitedQuery = resultQuery;
}

void QueryTokenizer::visitAndQuery(const AndQuery *query) {
    Query *resultQuery = new AndQuery(query->getQueryLabel());
    visitAndOrQuery(resultQuery, query);
}

void QueryTokenizer::visitOrQuery(const OrQuery *query) {
    Query *resultQuery = new OrQuery(query->getQueryLabel());
    visitAndOrQuery(resultQuery, query);
}

void QueryTokenizer::visitAndNotQuery(const AndNotQuery *query) {
    Query *resultQuery = new AndNotQuery(query->getQueryLabel());
    visitAndNotRankQuery(resultQuery, query);
}

void QueryTokenizer::visitRankQuery(const RankQuery *query) {
    Query *resultQuery = new RankQuery(query->getQueryLabel());
    visitAndNotRankQuery(resultQuery, query);
}

void QueryTokenizer::visitAndOrQuery(common::Query *resultQuery,
                                     const common::Query *srcQuery)
{
    const vector<QueryPtr> *childQueryPtr = srcQuery->getChildQuery();
    assert(childQueryPtr);
    vector<Query *> restQuery;
    size_t childNum = childQueryPtr->size();
    Query *query = nullptr;
    for (size_t i = 0; i < childNum; ++i) {
        if ((*childQueryPtr)[i]) {
            query = nullptr;
            (*childQueryPtr)[i]->accept(this);
            query = stealQuery();
            if (query) {
                restQuery.push_back(query);
            }
        }
    }
    if (_errorResult.hasError()) {
        for (auto it : restQuery) {
            delete it;
        }
        delete resultQuery;
        _visitedQuery = nullptr;
        return;
    }

    if (restQuery.size() > 1) {
        for (size_t j = 0; j < restQuery.size(); ++j) {
            QueryPtr queryPtr(restQuery[j]);
            resultQuery->addQuery(queryPtr);
        }
        _visitedQuery = resultQuery;
    } else if (1 == restQuery.size()) {
        delete resultQuery;
        _visitedQuery = restQuery[0];
        if (_visitedQuery->getQueryLabel().empty()) {
            _visitedQuery->setQueryLabelWithDefaultLevel(srcQuery->getQueryLabel());
        }
    } else {
        delete resultQuery;
        _visitedQuery = nullptr;
    }
}

void QueryTokenizer::visitAndNotRankQuery(common::Query *resultQuery,
                          const common::Query *srcQuery)
{
    const vector<QueryPtr> *childQueryPtr = srcQuery->getChildQuery();
    assert(childQueryPtr);
    vector<Query *> rightQuery;
    size_t childNum = childQueryPtr->size();
    Query *query = nullptr;
    Query *leftQuery = nullptr;
    if (childNum > 0) {
        if ((*childQueryPtr)[0]) {
            (*childQueryPtr)[0]->accept(this);
            leftQuery = stealQuery();
        }
    }

    if (_errorResult.hasError() || nullptr == leftQuery) {
        delete resultQuery;
        _visitedQuery = nullptr;
        if (!_errorResult.hasError()) {
            _errorResult.resetError(ERROR_QUERY_INVALID, "AndNotQuery or RankQuery's "
                    "leftQuery is nullptr or stop word.");
        }
        return;
    }

    for (size_t i = 1; i < childNum; ++i) {
        if ((*childQueryPtr)[i]) {
            query = nullptr;
            (*childQueryPtr)[i]->accept(this);
            query = stealQuery();
            if (query) {
                rightQuery.push_back(query);
            }
        }
    }
    if (_errorResult.hasError()) {
        for (auto it : rightQuery) {
            delete it;
        }
        delete leftQuery;
        delete resultQuery;
        _visitedQuery = nullptr;
        return;
    }

    if (rightQuery.size() > 0) {
        QueryPtr leftPtr(leftQuery);
        resultQuery->addQuery(leftPtr);
        for (size_t j = 0; j < rightQuery.size(); ++j) {
            QueryPtr rightPtr(rightQuery[j]);
            resultQuery->addQuery(rightPtr);
        }
        _visitedQuery = resultQuery;
    } else {
        delete resultQuery;
        _visitedQuery = leftQuery;
        if (leftQuery->getQueryLabel().empty()) {
            _visitedQuery->setQueryLabelWithDefaultLevel(srcQuery->getQueryLabel());
        }
    }
}

Query* QueryTokenizer::createTermQuery(const Token &token, const Term &term,
                                       const string &label) {
    const string &indexName = term.getIndexName();
    const RequiredFields &requiredFields = term.getRequiredFields();
    const int32_t boost = term.getBoost();
    const string &secondaryChain = term.getTruncateName();
    Query *query = new TermQuery(token, indexName.c_str(),
                                 requiredFields, label, boost, secondaryChain);

    AUTIL_LOG(DEBUG, "token : %s, pos : %d",
            token.getText().c_str(), (int32_t)token.getPosition());
    return query;
}

void QueryTokenizer::addMultiTerms(Query::TermArray &termArray,
        const TokenVect &tokens, const Term &term)
{
    const string &indexName = term.getIndexName();
    const RequiredFields &requiredFields = term.getRequiredFields();
    const int32_t boost = term.getBoost();
    const string &secondaryChain = term.getTruncateName();

    for (TokenVect::const_iterator it = tokens.begin();
         it != tokens.end(); ++it)
    {
        const Token &token = *it;
        TermPtr termPtr(new Term(token, indexName.c_str(),
                        requiredFields, boost, secondaryChain));
        termArray.push_back(termPtr);
    }
}

Query* QueryTokenizer::createDefaultQuery(const TokenVect &tokens, const Term &term,
                                          const string& label) {
    Query *query = nullptr;
    if (_defaultOP == OP_AND) {
        query = new AndQuery(label);
    } else if (_defaultOP == OP_OR) {
        query = new OrQuery(label);
    } else {
        AUTIL_LOG(WARN, "Unknow default query type!");
        return nullptr;
    }

    for (TokenVect::const_iterator it = tokens.begin();
         it != tokens.end(); ++it)
    {
        const Token &token = *it;
        Query *tmpQuery = createTermQuery(token, term, "");
        QueryPtr tmpQueryPtr(tmpQuery);
        query->addQuery(tmpQueryPtr);
    }

    return query;
}

Query* QueryTokenizer::stealQuery() {
    Query *tmpQuery = _visitedQuery;
    _visitedQuery = nullptr;
    return tmpQuery;
}

} // namespace common
} // namespace isearch
