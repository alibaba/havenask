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
#include "ha3/search/TermDFVisitor.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <limits>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "autil/Log.h"
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
#include "ha3/search/AuxiliaryChainDefine.h"

namespace isearch {
namespace common {
class Term;
} // namespace common
} // namespace isearch

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, TermDFVisitor);

using namespace isearch::common;
using namespace std;

TermDFVisitor::TermDFVisitor(const TermDFMap &auxListTerm)
    : _auxListTerm(auxListTerm)
    , _df(0) {}

TermDFVisitor::~TermDFVisitor() {}

void TermDFVisitor::visitTermQuery(const TermQuery *query) {
    const Term &term = query->getTerm();
    TermDFMap::const_iterator iter = _auxListTerm.find(term);
    if (iter != _auxListTerm.end()) {
        _df = iter->second;
    } else {
        _df = 0;
    }
}

void TermDFVisitor::visitPhraseQuery(const PhraseQuery *query) {
    df_t minDF = numeric_limits<df_t>::max();
    const PhraseQuery::TermArray &terms = query->getTermArray();
    for (PhraseQuery::TermArray::const_iterator it = terms.begin(); it != terms.end(); ++it) {
        df_t df = 0;
        TermDFMap::const_iterator iter = _auxListTerm.find(**it);
        if (iter != _auxListTerm.end()) {
            df = iter->second;
        }
        minDF = min(minDF, df);
    }
    _df = minDF;
}

void TermDFVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    df_t minDF = numeric_limits<df_t>::max();
    df_t sumDf = 0;
    const MultiTermQuery::TermArray &terms = query->getTermArray();
    for (MultiTermQuery::TermArray::const_iterator it = terms.begin(); it != terms.end(); ++it) {
        df_t df = 0;
        TermDFMap::const_iterator iter = _auxListTerm.find(**it);
        if (iter != _auxListTerm.end()) {
            df = iter->second;
        }
        minDF = min(minDF, df);
        sumDf += df;
    }
    _df = query->getOpExpr() == OP_AND ? minDF : sumDf;
}

void TermDFVisitor::visitAndQuery(const AndQuery *query) {
    df_t minDF = numeric_limits<df_t>::max();
    const vector<QueryPtr> *childQuerys = query->getChildQuery();
    for (size_t i = 0; i < childQuerys->size(); ++i) {
        (*childQuerys)[i]->accept(this);
        minDF = min(minDF, _df);
    }
    _df = minDF;
}

void TermDFVisitor::visitOrQuery(const OrQuery *query) {
    df_t sumDF = 0;
    const vector<QueryPtr> *childQuerys = query->getChildQuery();
    for (size_t i = 0; i < childQuerys->size(); ++i) {
        (*childQuerys)[i]->accept(this);
        sumDF += _df;
    }
    _df = sumDF;
}

void TermDFVisitor::visitAndNotQuery(const AndNotQuery *query) {
    const vector<QueryPtr> *childQuerys = query->getChildQuery();
    assert((*childQuerys)[0]);
    (*childQuerys)[0]->accept(this);
    // df_t df = _df;
    // for (size_t i = 1; i < childQuerys->size(); ++i) {
    //     (*childQuerys)[i]->accept(this);
    //     df -= _df;
    // }
    // _df = max(df, 0);
}

void TermDFVisitor::visitRankQuery(const RankQuery *query) {
    const vector<QueryPtr> *childQuerys = query->getChildQuery();
    assert((*childQuerys)[0]);
    (*childQuerys)[0]->accept(this);
}

void TermDFVisitor::visitNumberQuery(const NumberQuery *query) {
    const NumberTerm &term = query->getTerm();
    TermDFMap::const_iterator iter = _auxListTerm.find(term);
    if (iter != _auxListTerm.end()) {
        _df = iter->second;
    } else {
        _df = 0;
    }
}

} // namespace search
} // namespace isearch
