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
#include "ha3/search/SubMatchCheckVisitor.h"

#include <cstddef>
#include <string>
#include <memory>
#include <vector>

#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/QueryVisitor.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"


using namespace isearch::common;
using namespace std;

namespace isearch {
namespace search {

AUTIL_LOG_SETUP(ha3, SubMatchCheckVisitor);

#define VISIT_MID_QUERY(query) {                                        \
        if (!query->getQueryLabel().empty()) {                          \
            _needSubMatch = true;                                       \
        } else {                                                        \
            const vector<QueryPtr>* childQuerys = query->getChildQuery(); \
            for (size_t i = 0; i < childQuerys->size(); ++i) {          \
                if (!_needSubMatch) {                                   \
                    (*childQuerys)[i]->accept(this);                    \
                }                                                       \
            }                                                           \
        }                                                               \
    }

SubMatchCheckVisitor::SubMatchCheckVisitor()
    : _needSubMatch(false)
{
}

void SubMatchCheckVisitor::visitTermQuery(const TermQuery *query) {
    if (!query->getQueryLabel().empty()) {
        _needSubMatch = true;
    }
}

void SubMatchCheckVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    if (!query->getQueryLabel().empty()) {
        _needSubMatch = true;
    }
}

void SubMatchCheckVisitor::visitPhraseQuery(const PhraseQuery *query) {
   if (!query->getQueryLabel().empty()) {
        _needSubMatch = true;
   }
}

void SubMatchCheckVisitor::visitAndQuery(const AndQuery *query) {
    VISIT_MID_QUERY(query)
}

void SubMatchCheckVisitor::visitOrQuery(const OrQuery *query) {
    VISIT_MID_QUERY(query)
}

void SubMatchCheckVisitor::visitAndNotQuery(const AndNotQuery *query) {
    VISIT_MID_QUERY(query)
}

void SubMatchCheckVisitor::visitRankQuery(const RankQuery *query) {
    VISIT_MID_QUERY(query)
}

void SubMatchCheckVisitor::visitNumberQuery(const NumberQuery *query) {
    if (!query->getQueryLabel().empty()) {
        _needSubMatch = true;
    }
}

}
}
