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
#include "ha3/common/QueryFlatten.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <typeinfo>
#include <vector>

#include "autil/Log.h"
#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"

using namespace std;
using namespace isearch::common;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, QueryFlatten);

QueryFlatten::QueryFlatten() {
    _visitedQuery = NULL;
}

QueryFlatten::~QueryFlatten() {
    DELETE_AND_SET_NULL(_visitedQuery);
}

void QueryFlatten::visitTermQuery(const TermQuery *query) {
    assert(!_visitedQuery);
    _visitedQuery = new TermQuery(*query);
}

void QueryFlatten::visitPhraseQuery(const PhraseQuery *query) {
    assert(!_visitedQuery);
    _visitedQuery = new PhraseQuery(*query);
}

void QueryFlatten::visitMultiTermQuery(const MultiTermQuery *query) {
    assert(!_visitedQuery);
    _visitedQuery = new MultiTermQuery(*query);
}

void QueryFlatten::flatten(Query *resultQuery, const Query *srcQuery) {
    const vector<QueryPtr> *childrenQuery = srcQuery->getChildQuery();
    assert(childrenQuery->size() > 1);

    vector<QueryPtr>::const_iterator it = childrenQuery->begin();
    const QueryPtr &leftChild = *it;
    auto &leftChild_raw = *leftChild;
    auto &srcQuery_raw = *srcQuery;
    if (typeid(leftChild_raw) == typeid(srcQuery_raw)
        && MDL_SUB_QUERY != leftChild->getMatchDataLevel()) {
        flatten(resultQuery, leftChild.get());
    } else {
        leftChild->accept(this);
        QueryPtr visitedQueryPtr(stealQuery());
        resultQuery->addQuery(visitedQueryPtr);
    }

    bool bFlattenRightChild
        = typeid(srcQuery_raw) == typeid(AndQuery) || typeid(srcQuery_raw) == typeid(OrQuery);
    for (it++; it != childrenQuery->end(); it++) {
        auto &childrenQueryItem_raw = *(*it);
        if (bFlattenRightChild && typeid(srcQuery_raw) == typeid(childrenQueryItem_raw)
            && MDL_SUB_QUERY != (*it)->getMatchDataLevel()) {
            flatten(resultQuery, (*it).get());
        } else {
            (*it)->accept(this);
            QueryPtr visitedQueryPtr(stealQuery());
            resultQuery->addQuery(visitedQueryPtr);
        }
    }
}

void QueryFlatten::visitAndQuery(const AndQuery *query) {
    AndQuery *resultQuery = new AndQuery(query->getQueryLabel());
    flatten(resultQuery, query);
    _visitedQuery = resultQuery;
}

void QueryFlatten::visitOrQuery(const OrQuery *query) {
    OrQuery *resultQuery = new OrQuery(query->getQueryLabel());
    flatten(resultQuery, query);
    _visitedQuery = resultQuery;
}

void QueryFlatten::visitAndNotQuery(const AndNotQuery *query) {
    AndNotQuery *resultQuery = new AndNotQuery(query->getQueryLabel());
    flatten(resultQuery, query);
    _visitedQuery = resultQuery;
}

void QueryFlatten::visitRankQuery(const RankQuery *query) {
    RankQuery *resultQuery = new RankQuery(query->getQueryLabel());
    flatten(resultQuery, query);
    _visitedQuery = resultQuery;
}

void QueryFlatten::visitNumberQuery(const NumberQuery *query) {
    assert(!_visitedQuery);
    _visitedQuery = new NumberQuery(*query);
}

Query *QueryFlatten::stealQuery() {
    Query *tmpQuery = _visitedQuery;
    _visitedQuery = NULL;
    return tmpQuery;
}

} // namespace common
} // namespace isearch
