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
#include <stdint.h>

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
} // namespace common
} // namespace isearch

namespace isearch {
namespace common {

class QueryTermVisitor : public QueryVisitor {
public:
    typedef int32_t VisitTermType;
    static const VisitTermType VT_RANK_QUERY = 1;
    static const VisitTermType VT_ANDNOT_QUERY = 2;
    static const VisitTermType VT_NO_AUX_TERM = 0;
    static const VisitTermType VT_ALL = 0xffff;

public:
    QueryTermVisitor(VisitTermType visitType = VT_RANK_QUERY);
    virtual ~QueryTermVisitor();

public:
    virtual void visitTermQuery(const TermQuery *query);
    virtual void visitPhraseQuery(const PhraseQuery *query);
    virtual void visitAndQuery(const AndQuery *query);
    virtual void visitOrQuery(const OrQuery *query);
    virtual void visitAndNotQuery(const AndNotQuery *query);
    virtual void visitRankQuery(const RankQuery *query);
    virtual void visitNumberQuery(const NumberQuery *query);
    virtual void visitMultiTermQuery(const MultiTermQuery *query);
    const TermVector &getTermVector() const {
        return _termVector;
    }

private:
    void visitAdvancedQuery(const Query *query);

private:
    TermVector _termVector;
    VisitTermType _visitTermType;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryTermVisitor> QueryTermVisitorPtr;

} // namespace common
} // namespace isearch
