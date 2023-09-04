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
#include "ha3/common/QueryTermVisitor.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/TermQuery.h"

namespace isearch {
namespace common {
class NumberQuery;
} // namespace common
} // namespace isearch

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, QueryTermVisitor);

QueryTermVisitor::QueryTermVisitor(VisitTermType visitType) {
    _visitTermType = visitType;
}

QueryTermVisitor::~QueryTermVisitor() {}

void QueryTermVisitor::visitTermQuery(const TermQuery *query) {
    const Term &term = query->getTerm();
    _termVector.push_back(term);
}

void QueryTermVisitor::visitPhraseQuery(const PhraseQuery *query) {
    const Query::TermArray &termArray = query->getTermArray();
    for (size_t i = 0; i < termArray.size(); ++i) {
        _termVector.push_back(*termArray[i]);
    }
}

void QueryTermVisitor::visitAndQuery(const AndQuery *query) {
    visitAdvancedQuery(query);
}

void QueryTermVisitor::visitOrQuery(const OrQuery *query) {
    visitAdvancedQuery(query);
}

void QueryTermVisitor::visitAndNotQuery(const AndNotQuery *query) {
    const vector<QueryPtr> *children = query->getChildQuery();
    assert(children);
    assert(children->size() > 0);
    (*children)[0]->accept(this);
    if (_visitTermType & VT_ANDNOT_QUERY) {
        for (size_t i = 1; i < children->size(); ++i) {
            (*children)[i]->accept(this);
        }
    }
}

void QueryTermVisitor::visitRankQuery(const RankQuery *query) {
    const vector<QueryPtr> *children = query->getChildQuery();
    assert(children);
    assert(children->size() > 0);
    (*children)[0]->accept(this);
    if (_visitTermType & VT_RANK_QUERY) {
        for (size_t i = 1; i < children->size(); ++i) {
            (*children)[i]->accept(this);
        }
    }
}

void QueryTermVisitor::visitNumberQuery(const NumberQuery *query) {
    ;
}

void QueryTermVisitor::visitAdvancedQuery(const Query *query) {
    const vector<QueryPtr> *children = query->getChildQuery();
    assert(children);
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
    }
}

void QueryTermVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    const Query::TermArray &termArray = query->getTermArray();
    for (size_t i = 0; i < termArray.size(); ++i) {
        _termVector.push_back(*termArray[i]);
    }
}

} // namespace common
} // namespace isearch
