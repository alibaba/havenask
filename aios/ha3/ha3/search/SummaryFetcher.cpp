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
#include "ha3/search/SummaryFetcher.h"

#include <assert.h>
#include <iosfwd>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "ha3/common/AttributeItem.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

using namespace std;
using namespace suez::turing;
using namespace isearch::common;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SummaryFetcher);

void SummaryFetcher::batchFillAttributeToSummary(
    const indexlib::index::ErrorCodeVec &summaryEc,
    const std::vector<docid_t> &docIds,
    const indexlib::index::SearchSummaryDocVec &summaryDocVec) {
    assert(_matchDocAllocator);
    if (_attributes.size() == 0) {
        return;
    }
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (summaryEc[i] != indexlib::index::ErrorCode::OK) {
            continue;
        }

        auto matchDoc = _matchDocAllocator->allocate(docIds[i]);
        map<summaryfieldid_t, AttributeExpression *>::iterator it = _attributes.begin();
        for (; it != _attributes.end(); ++it) {
            AttributeExpression *attrExpr = it->second;
            attrExpr->evaluate(matchDoc);
            auto reference = attrExpr->getReferenceBase();
            string str = reference->toString(matchDoc);
            summaryDocVec[i]->SetFieldValue(it->first, str.c_str(), str.size());
        }
        _matchDocAllocator->deallocate(matchDoc);
    }
}

} // namespace search
} // namespace isearch

