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
#include "ha3/search/AuxiliaryChainVisitor.h"

#include <assert.h>
#include <cstddef>
#include <limits>
#include <map>
#include <memory>
#include <stdint.h>
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
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"
#include "ha3/search/AuxiliaryChainDefine.h"
#include "ha3/search/TermDFVisitor.h"
#include "indexlib/indexlib.h"

using namespace std;

using namespace isearch::common;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, AuxiliaryChainVisitor);

AuxiliaryChainVisitor::AuxiliaryChainVisitor(const std::string &auxChainName,
                                             const TermDFMap &termDFMap,
                                             SelectAuxChainType type)
    : _auxChainName(auxChainName)
    , _termDFMap(termDFMap)
    , _type(type) {}

AuxiliaryChainVisitor::~AuxiliaryChainVisitor() {}

void AuxiliaryChainVisitor::visitTermQuery(TermQuery *query) {
    common::Term &term = query->getTerm();
    term.setTruncateName(_auxChainName);
}

void AuxiliaryChainVisitor::visitPhraseQuery(PhraseQuery *query) {
    PhraseQuery::TermArray &terms = query->getTermArray();
    for (size_t i = 0; i < terms.size(); ++i) {
        terms[i]->setTruncateName(_auxChainName);
    }
}

void AuxiliaryChainVisitor::visitMultiTermQuery(MultiTermQuery *query) {
    MultiTermQuery::TermArray &terms = query->getTermArray();
    QueryOperator op = query->getOpExpr();
    if (op == OP_AND && _type != SAC_ALL) {
        int32_t maxIdx = -1;
        int32_t minIdx = -1;
        df_t maxDF = 0;
        df_t minDF = numeric_limits<df_t>::max();
        for (int32_t i = 0; i < (int32_t)terms.size(); ++i) {
            TermDFMap::const_iterator iter = _termDFMap.find(*terms[i]);
            df_t df = (iter != _termDFMap.end()) ? iter->second : 0;
            if (df > maxDF) {
                maxIdx = i;
                maxDF = df;
            }
            if (df < minDF) {
                minIdx = i;
                minDF = df;
            }
        }
        if (maxIdx >= 0 && _type == SAC_DF_BIGGER) {
            terms[maxIdx]->setTruncateName(_auxChainName);
        } else if (minIdx >= 0 && _type == SAC_DF_SMALLER) {
            terms[minIdx]->setTruncateName(_auxChainName);
        }
    } else { // OP_OR, OP_WEAKAND
        for (size_t i = 0; i < terms.size(); ++i) {
            terms[i]->setTruncateName(_auxChainName);
        }
    }
}

void AuxiliaryChainVisitor::visitAndQuery(AndQuery *query) {
    vector<QueryPtr> *childQuerys = query->getChildQuery();
    if (_type == SAC_ALL) {
        for (int32_t i = 0; i < (int32_t)childQuerys->size(); ++i) {
            (*childQuerys)[i]->accept(this);
        }
        return;
    }
    int32_t maxIdx = -1;
    int32_t minIdx = -1;
    df_t maxDF = 0;
    df_t minDF = numeric_limits<df_t>::max();
    for (int32_t i = 0; i < (int32_t)childQuerys->size(); ++i) {
        TermDFVisitor dfVistor(_termDFMap);
        (*childQuerys)[i]->accept(&dfVistor);
        df_t df = dfVistor.getDF();
        if (df > maxDF) {
            maxIdx = i;
            maxDF = df;
        }
        if (df < minDF) {
            minIdx = i;
            minDF = df;
        }
    }
    if (maxIdx >= 0 && _type == SAC_DF_BIGGER) {
        (*childQuerys)[maxIdx]->accept(this);
    } else if (minIdx >= 0 && _type == SAC_DF_SMALLER) {
        (*childQuerys)[minIdx]->accept(this);
    }
}

void AuxiliaryChainVisitor::visitOrQuery(OrQuery *query) {
    vector<QueryPtr> *childQuerys = query->getChildQuery();
    for (size_t i = 0; i < childQuerys->size(); ++i) {
        (*childQuerys)[i]->accept(this);
    }
}

void AuxiliaryChainVisitor::visitAndNotQuery(AndNotQuery *query) {
    vector<QueryPtr> *childQuerys = query->getChildQuery();
    assert((*childQuerys)[0]);
    (*childQuerys)[0]->accept(this);
}

void AuxiliaryChainVisitor::visitRankQuery(RankQuery *query) {
    vector<QueryPtr> *childQuerys = query->getChildQuery();
    assert((*childQuerys)[0]);
    (*childQuerys)[0]->accept(this);
}

void AuxiliaryChainVisitor::visitNumberQuery(NumberQuery *query) {
    common::NumberTerm &term = query->getTerm();
    term.setTruncateName(_auxChainName);
}

} // namespace search
} // namespace isearch
