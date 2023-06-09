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
#include "ha3/qrs/TransAnd2OrVisitor.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <vector>


#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;
using namespace isearch::common;
namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, TransAnd2OrVisitor);

TransAnd2OrVisitor::TransAnd2OrVisitor() {
}

TransAnd2OrVisitor::~TransAnd2OrVisitor() {
}

void TransAnd2OrVisitor::visitTermQuery(const TermQuery *query){
    _query = query->clone();
}

void TransAnd2OrVisitor::visitPhraseQuery(const PhraseQuery *query){
    const PhraseQuery::TermArray &termArray = query->getTermArray();
    Query *orQuery = new OrQuery(query->getQueryLabel());
    for (size_t i = 0; i < termArray.size(); ++i) {
        QueryPtr termQueryPtr(new TermQuery(*termArray[i], ""));
        orQuery->addQuery(termQueryPtr);
    }
    _query = orQuery;
}

void TransAnd2OrVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    MultiTermQuery *multiTermQuery = new MultiTermQuery(*query);
    multiTermQuery->setOPExpr(OP_OR);
    _query = multiTermQuery;
}

void TransAnd2OrVisitor::visitAndQuery(const AndQuery *query){
    visitAdvancedQuery(query);
}

void TransAnd2OrVisitor::visitOrQuery(const OrQuery *query){
    visitAdvancedQuery(query);
}

void TransAnd2OrVisitor::visitAndNotQuery(const AndNotQuery *query){
    visitAdvancedQuery(query);
}

void TransAnd2OrVisitor::visitRankQuery(const RankQuery *query){
    visitAdvancedQuery(query);
}

void TransAnd2OrVisitor::visitNumberQuery(const NumberQuery *query){
    _query = query->clone();
}
Query *TransAnd2OrVisitor::stealQuery(){
    Query *tmp = _query;
    _query = NULL;
    return tmp;
}
void TransAnd2OrVisitor::visitAdvancedQuery(const Query *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    OrQuery *orQuery = new OrQuery(query->getQueryLabel());
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
        QueryPtr childQueryPtr(stealQuery());
        orQuery->addQuery(childQueryPtr);
    }
    _query = orQuery;
}

} // namespace qrs
} // namespace isearch

