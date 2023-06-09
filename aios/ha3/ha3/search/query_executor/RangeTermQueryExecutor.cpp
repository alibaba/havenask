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
#include "ha3/search/RangeTermQueryExecutor.h"

#include <cstddef>

#include "autil/Log.h"
#include "ha3/isearch.h"
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/inverted_index/RangePostingIterator.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"

namespace indexlib {
namespace index {
class PostingIterator;
} // namespace index
} // namespace indexlib
namespace isearch {
namespace common {
class Term;
} // namespace common
} // namespace isearch

using namespace std;
using namespace indexlib::index;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, RangeTermQueryExecutor);

RangeTermQueryExecutor::RangeTermQueryExecutor(indexlib::index::PostingIterator *iter,
                                               const common::Term &term)
    : TermQueryExecutor(iter, term)
    , _filter(NULL) {
    initRangePostingIterator();
}

RangeTermQueryExecutor::~RangeTermQueryExecutor() {}

uint32_t RangeTermQueryExecutor::getSeekDocCount() { return _rangeIter->GetSeekDocCount(); }

indexlib::index::ErrorCode RangeTermQueryExecutor::doSeek(docid_t id, docid_t &result) {
    docid_t tempDocId = INVALID_DOCID;
    auto ec = _rangeIter->InnerSeekDoc(id, tempDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    if (tempDocId == INVALID_DOCID) {
        tempDocId = END_DOCID;
    }
    result = tempDocId;
    return indexlib::index::ErrorCode::OK;
}

void RangeTermQueryExecutor::initRangePostingIterator() {
    SeekAndFilterIterator *compositeIter = (SeekAndFilterIterator *)_iter;
    _rangeIter = (indexlib::index::RangePostingIterator *)compositeIter->GetIndexIterator();
    _filter = compositeIter->GetDocValueFilter();
}

} // namespace search
} // namespace isearch
