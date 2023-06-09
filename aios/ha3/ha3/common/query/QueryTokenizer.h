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

#include <assert.h>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/analyzer/Token.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"
#include "ha3/isearch.h"

namespace build_service {
namespace analyzer {
class AnalyzerFactory;
}  // namespace analyzer
}  // namespace build_service
namespace isearch {
namespace common {
class AndNotQuery;
class AndQuery;
class NumberQuery;
class OrQuery;
class PhraseQuery;
class RankQuery;
class Term;
class TermQuery;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class IndexInfos;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace common {
class MultiTermQuery;

class QueryTokenizer : public common::QueryVisitor
{
public:
    typedef std::vector<build_service::analyzer::Token> TokenVect;

public:
    QueryTokenizer(const build_service::analyzer::AnalyzerFactory *analyzerFactory);
    QueryTokenizer(const QueryTokenizer &tokenzier);
    virtual ~QueryTokenizer();

public:
    void setConfigInfo(const std::set<std::string>& indexes,
                       const std::string &analyzerName,
                       const std::map<std::string, std::string> &indexMap)
    {
        _noTokenizeIndexes.clear();
        _indexAnalyzerMap.clear();
        _noTokenizeIndexes.insert(indexes.begin(), indexes.end());
        _indexAnalyzerMap.insert(indexMap.begin(), indexMap.end());
        setGlobalAnalyzerName(analyzerName);
    }
    void setNoTokenizeIndexes(const std::unordered_set<std::string>& indexes) {
        _noTokenizeIndexes = indexes;
    }
    void setGlobalAnalyzerName(const std::string &analyzerName) {
        _globalAnalyzerName = analyzerName;
    }
    void addIndexAnalyzerName(const std::string& indexName,
                              const std::string& analyzerName) {
        _indexAnalyzerMap[indexName] = analyzerName;
    }
    void setIndexAnalyzerNameMap(const std::unordered_map<std::string, std::string> &indexMap) {
        _indexAnalyzerMap = indexMap;
    }
    std::string getIndexAnalyzerName(const std::string& indexName) const {
        auto it = _indexAnalyzerMap.find(indexName);
        if (it != _indexAnalyzerMap.end()) {
            return it->second;
        }
        return _globalAnalyzerName;
    }

    bool tokenizeQuery(const common::Query *query,
                       const suez::turing::IndexInfos *indexInfos,
                       QueryOperator defaultOP = OP_AND,
                       bool needCleanStopWords = false)
    {
        _errorResult.resetError(ERROR_NONE);
        _indexInfos = indexInfos;
        _defaultOP = defaultOP;
        _needCleanStopWords = needCleanStopWords;
        DELETE_AND_SET_NULL(_visitedQuery);
        assert(query);
        query->accept(this);
        return !_errorResult.hasError();
    }
public:
    virtual void visitTermQuery(const common::TermQuery *query);
    virtual void visitPhraseQuery(const common::PhraseQuery *query);
    virtual void visitAndQuery(const common::AndQuery *query);
    virtual void visitOrQuery(const common::OrQuery *query);
    virtual void visitAndNotQuery(const common::AndNotQuery *query);
    virtual void visitRankQuery(const common::RankQuery *query);
    virtual void visitNumberQuery(const common::NumberQuery *query);
    virtual void visitMultiTermQuery(const common::MultiTermQuery *query);
    common::Query* stealQuery();
    const common::ErrorResult getErrorResult() const { return _errorResult; }

private:
    void tokenize(const std::string &indexName, const common::Term &term,
                  TokenVect &tokens, bool needCleanStopWords = false);
    bool needTokenize(const std::string &indexName);
    void doTokenize(const std::string &indexName, const std::string &word,
                    TokenVect &tokens, bool needTokenize);
    void reConstructBinaryQuery(const common::MultiTermQuery *query);
    void visitAndOrQuery(common::Query *resultQuery,
                         const common::Query *srcQuery);
    void visitAndNotRankQuery(common::Query *resultQuery,
                         const common::Query *srcQuery);

    common::Query* createTermQuery(const build_service::analyzer::Token &token,
                                   const common::Term &term,
                                   const std::string &label);
    common::Query* createDefaultQuery(const TokenVect &tokens,
            const common::Term &term, const std::string &label);
    void addMultiTerms(common::Query::TermArray &termArray,
                       const TokenVect &tokens, const common::Term &term);
    bool isIndexNoTokenize(const std::string &indexName) const {
        return _noTokenizeIndexes.find(indexName) != _noTokenizeIndexes.end();
    }

private:
    const suez::turing::IndexInfos *_indexInfos;
    QueryOperator _defaultOP;
    const build_service::analyzer::AnalyzerFactory *_analyzerFactory;
    common::Query *_visitedQuery;
    common::ErrorResult _errorResult;
    std::string _globalAnalyzerName;
    std::unordered_set<std::string> _noTokenizeIndexes;
    std::unordered_map<std::string, std::string> _indexAnalyzerMap;
    bool _needCleanStopWords;
private:
    friend class QueryTokenizerTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryTokenizer> QueryTokenizerPtr;

} // namespace common
} // namespace isearch
