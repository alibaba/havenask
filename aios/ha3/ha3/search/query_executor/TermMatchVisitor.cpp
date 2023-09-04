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
#include "ha3/search/TermMatchVisitor.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

using namespace isearch::common;
using namespace indexlib::config;
using namespace std;

namespace isearch {
namespace search {

AUTIL_LOG_SETUP(ha3, TermMatchVisitor);

void TermMatchVisitor::visitMidQuery(Query *query) {
    vector<QueryPtr> *childQuerys = query->getChildQuery();
    for (size_t i = 0; i < childQuerys->size(); ++i) {
        (*childQuerys)[i]->accept(this);
    }
}

void TermMatchVisitor::visitLeafQuery(const std::string &indexName, Query *query) {
    if (query->getQueryLabel().empty()) {
        auto indexConfig = dynamic_cast<indexlibv2::config::InvertedIndexConfig *>(
            _indexSchemaPtr
                ->GetIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, indexName)
                .get());
        if (!indexConfig
            || ((indexConfig->GetOptionFlag() & OPTION_FLAG_ALL) == OPTION_FLAG_NONE)) {
            query->setMatchDataLevel(MDL_NONE);
        } else {
            query->setMatchDataLevel(MDL_TERM);
        }
    }
}

TermMatchVisitor::TermMatchVisitor(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &indexSchemaPtr)
    : _indexSchemaPtr(indexSchemaPtr) {}

void TermMatchVisitor::visitTermQuery(TermQuery *query) {
    const string &indexName = query->getTerm().getIndexName();
    visitLeafQuery(indexName, query);
}

void TermMatchVisitor::visitMultiTermQuery(MultiTermQuery *query) {
    const auto &terms = query->getTermArray();
    if (terms.empty()) {
        return;
    }
    const string &indexName = terms[0]->getIndexName();
    visitLeafQuery(indexName, query);
}

void TermMatchVisitor::visitPhraseQuery(PhraseQuery *query) {
    const auto &terms = query->getTermArray();
    if (terms.empty()) {
        return;
    }
    const string &indexName = terms[0]->getIndexName();
    visitLeafQuery(indexName, query);
}

void TermMatchVisitor::visitAndQuery(AndQuery *query) {
    visitMidQuery(query);
}

void TermMatchVisitor::visitOrQuery(OrQuery *query) {
    visitMidQuery(query);
}

void TermMatchVisitor::visitAndNotQuery(AndNotQuery *query) {
    visitMidQuery(query);
}

void TermMatchVisitor::visitRankQuery(RankQuery *query) {
    visitMidQuery(query);
}

void TermMatchVisitor::visitNumberQuery(NumberQuery *query) {
    const string &indexName = query->getTerm().getIndexName();
    visitLeafQuery(indexName, query);
}

} // namespace search
} // namespace isearch
