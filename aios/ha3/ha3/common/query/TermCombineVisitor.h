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

#include <memory>
#include <tuple>

#include "autil/CommonMacros.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/json.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"

namespace indexlibv2 {
namespace config {
class ITabletSchema;
}
} // namespace indexlibv2

namespace isearch {
namespace common {
class AndNotQuery;
class AndQuery;
class MultiTermQuery;
class NumberQuery;
class OrQuery;
class PhraseQuery;
class RankQuery;
class TermQuery;
} // namespace common
} // namespace isearch

namespace isearch {
namespace common {

class TermCombineVisitor : public common::QueryVisitor {
public:
    static const std::string DEFAULT_TOKEN_CONNECTOR;
    struct SingleTerms {
        SingleTerms() {
            reset();
        }
        SingleTerms(QueryOperator op_, const std::vector<Term> &terms_)
            : op(op_)
            , terms(terms_) {}
        SingleTerms(QueryOperator op_, const Term &term_)
            : op(op_)
            , terms({term_}) {}
        void reset() {
            terms.clear();
        }
        QueryOperator op;
        std::vector<Term> terms;
    };

public:
    TermCombineVisitor(const autil::legacy::json::JsonMap &param);
    virtual ~TermCombineVisitor();

public:
    bool hasCombineDict() {
        return !_combineDict.empty();
    }

    virtual void visitTermQuery(const common::TermQuery *query);
    virtual void visitPhraseQuery(const common::PhraseQuery *query);
    virtual void visitAndQuery(const common::AndQuery *query);
    virtual void visitOrQuery(const common::OrQuery *query);
    virtual void visitAndNotQuery(const common::AndNotQuery *query);
    virtual void visitRankQuery(const common::RankQuery *query);
    virtual void visitNumberQuery(const common::NumberQuery *query);
    virtual void visitMultiTermQuery(const common::MultiTermQuery *query);

    common::Query *stealQuery();

private:
    void createDict(const autil::legacy::json::JsonMap &param);
    bool match(const std::vector<SingleTerms> &childQuerySingleTerms,
               const std::vector<std::string> &fields,
               std::vector<bool> &masks,
               std::vector<SingleTerms> &matchedResult);
    void createCombineIndex(const std::vector<SingleTerms> &matchedResult,
                            const std::string &indexName,
                            const std::string &tokenConnector,
                            common::AndQuery *resultQuery);
    void getWordsFromMatchedResult(const std::vector<SingleTerms> &matchedResult,
                                   std::vector<std::string> &words,
                                   const std::string &tokenConnector);
    template <class T>
    inline void visitChildren(const T *query);

private:
    common::Query *_visitedQuery;
    std::vector<std::tuple<std::string, std::vector<std::string>, std::string>> _combineDict;
    SingleTerms _singleTerms;

private:
    AUTIL_LOG_DECLARE();
    friend class TermCombineVisitorTest;
};

typedef std::shared_ptr<TermCombineVisitor> TermCombineVisitorPtr;

} // namespace common
} // namespace isearch
