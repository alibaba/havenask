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
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/QueryVisitor.h"
#include "ha3/common/Term.h"

namespace isearch {
namespace common {
class AndNotQuery;
class AndQuery;
class MultiTermQuery;
class NumberQuery;
class OrQuery;
class PhraseQuery;
class Query;
class RankQuery;
class TermQuery;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace qrs {

class ParsedQueryVisitor : public common::QueryVisitor
{
public:
    ParsedQueryVisitor();
    virtual ~ParsedQueryVisitor();
public:
    virtual void visitTermQuery(const common::TermQuery *query);
    virtual void visitPhraseQuery(const common::PhraseQuery *query);
    virtual void visitAndQuery(const common::AndQuery *query);
    virtual void visitOrQuery(const common::OrQuery *query);
    virtual void visitAndNotQuery(const common::AndNotQuery *query);
    virtual void visitRankQuery(const common::RankQuery *query);
    virtual void visitNumberQuery(const common::NumberQuery *query);
    virtual void visitMultiTermQuery(const common::MultiTermQuery *query);

    const std::string& getParsedQueryStr() const {
        return _queryStr;
    }
private:
    void visitAdvancedQuery(const common::Query *query, const std::string& prefix);
    void addTerm(const common::Term& term, std::string& str);
    void visitTermArray(const std::vector<common::TermPtr> &termArray, 
                        std::string &str);
private:
    std::string _queryStr;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ParsedQueryVisitor> ParsedQueryVisitorPtr;

} // namespace qrs
} // namespace isearch

