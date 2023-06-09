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
#include "ha3/qrs/ParsedQueryVisitor.h"

#include <assert.h>
#include <cstddef>
#include <memory>


#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/NumberTerm.h"
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
AUTIL_LOG_SETUP(ha3, ParsedQueryVisitor);

ParsedQueryVisitor::ParsedQueryVisitor() {
}

ParsedQueryVisitor::~ParsedQueryVisitor() {
}
void ParsedQueryVisitor::addTerm(const Term& term, std::string& str){
    str.append(term.getIndexName());
    str.append(":");
    str.append(term.getWord().c_str());
}

void ParsedQueryVisitor::visitTermQuery(const TermQuery *query){
    const Term &term = query->getTerm();
    addTerm(term, _queryStr);
}

void ParsedQueryVisitor::visitPhraseQuery(const PhraseQuery *query){
    const PhraseQuery::TermArray &termArray = query->getTermArray();
    string str ="PHRASE(";
    visitTermArray(termArray, str);
}

void ParsedQueryVisitor::visitMultiTermQuery(const MultiTermQuery *query){
    const MultiTermQuery::TermArray &termArray = query->getTermArray();
    string str ="MULTI TERM";
    if (query->getOpExpr() == OP_AND) {
        str.append("<AND>(");
    } else if (query->getOpExpr() == OP_OR) {
        str.append("<OR><");
    } else if (query->getOpExpr() == OP_WEAKAND) {
        str.append("<WEAKAND><");
    }
    visitTermArray(termArray, str);
}

void ParsedQueryVisitor::visitAndQuery(const AndQuery *query){
    visitAdvancedQuery(query, "AND");
}

void ParsedQueryVisitor::visitOrQuery(const OrQuery *query){
    visitAdvancedQuery(query, "OR");
}

void ParsedQueryVisitor::visitAndNotQuery(const AndNotQuery *query){
    visitAdvancedQuery(query,"NOT");
}

void ParsedQueryVisitor::visitRankQuery(const RankQuery *query){
    visitAdvancedQuery(query,"RANK");
}

void ParsedQueryVisitor::visitNumberQuery(const NumberQuery *query){
    const NumberTerm &term = query->getTerm();
    addTerm(term, _queryStr);
}

void ParsedQueryVisitor::visitAdvancedQuery(const Query *query, const string& prefix) {
    _queryStr += prefix + "(";
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
        _queryStr.append(",");
    }
    if(_queryStr[_queryStr.size() - 1] == ','){
        _queryStr.erase(_queryStr.end() - 1);
    }
    _queryStr.append(")");
}

void ParsedQueryVisitor::visitTermArray(const vector<TermPtr> &termArray,
                                        string &str)
{
    for (size_t i = 0; i < termArray.size(); ++i) {
        addTerm(*termArray[i], str);
        str.append(",");
    }
    if(str[str.size() - 1] == ','){
        str.erase(str.end() - 1);
    }
    str.append(")");
    _queryStr.append(str);
}

} // namespace qrs
} // namespace isearch

