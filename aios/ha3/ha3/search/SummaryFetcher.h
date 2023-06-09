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

#include <map>
#include <memory>

#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

#include "ha3/common/Hit.h"
#include "ha3/common/Hits.h"
#include "ha3/common/SummaryHit.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace indexlib {
namespace document {
class SearchSummaryDocument;
}  // namespace document
}  // namespace indexlib
namespace matchdoc {
class MatchDocAllocator;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class SummaryFetcher
{
public:
    SummaryFetcher(const std::map<summaryfieldid_t, suez::turing::AttributeExpression*>
                   &attributes,
                   matchdoc::MatchDocAllocator *matchDocAllocator,
                   indexlib::index::SummaryReaderPtr summaryReader)
        : _attributes(attributes)
        , _matchDocAllocator(matchDocAllocator)
        , _summaryReader(summaryReader)
    {
    }
    virtual ~SummaryFetcher() {_attributes.clear();}
private:
    SummaryFetcher(const SummaryFetcher &);
    SummaryFetcher& operator=(const SummaryFetcher &);
    void batchFillAttributeToSummary(const indexlib::index::ErrorCodeVec &summaryEc,
                                     const std::vector<docid_t> &docid,
                                     const indexlib::index::SearchSummaryDocVec&summaryDocVec);
public:
    indexlib::index::ErrorCodeVec batchFetchSummary(common::Hits *hits,
                                                     autil::mem_pool::Pool *sessionPool,
                                                     autil::TimeoutTerminator* terminator,
                                                     const SummaryGroupIdVec &summaryGroupIdVec) {
        uint32_t hitSize = hits->size();
        std::vector<docid_t> docIds;
        indexlib::index::SearchSummaryDocVec summaryDocVec;
        indexlib::index::ErrorCodeVec result(hitSize, indexlib::index::ErrorCode::OK);
        docIds.reserve(hitSize);
        summaryDocVec.reserve(hitSize);
        for (size_t i = 0; i < hitSize; ++i) {
            common::Hit *hit = hits->getHit(i).get();
            docid_t docId = hit->getDocId();
            docIds.push_back(docId);
            summaryDocVec.push_back(hit->getSummaryHit()->getSummaryDocument());
        }
        if (_summaryReader) {
            if (summaryGroupIdVec.empty()) {
                result = future_lite::coro::syncAwait(
                    _summaryReader->GetDocument(docIds, sessionPool, terminator, &summaryDocVec));
            } else {
                result = future_lite::coro::syncAwait(_summaryReader->GetDocument(
                    docIds, summaryGroupIdVec, sessionPool, terminator, &summaryDocVec));
            }
        }
        batchFillAttributeToSummary(result, docIds, summaryDocVec);
        return result;
    }

private:
    std::map<summaryfieldid_t, suez::turing::AttributeExpression*> _attributes;
    matchdoc::MatchDocAllocator *_matchDocAllocator;
    indexlib::index::SummaryReaderPtr _summaryReader;
protected:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryFetcher> SummaryFetcherPtr;
} // namespace search
} // namespace isearch

