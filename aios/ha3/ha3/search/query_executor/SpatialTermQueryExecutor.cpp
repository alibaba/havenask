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
#include "ha3/search/SpatialTermQueryExecutor.h"

#include <cstddef>

#include "autil/Log.h"
#include "ha3/isearch.h"
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialPostingIterator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"

namespace isearch {
namespace common {
class Term;
} // namespace common
} // namespace isearch

using namespace std;
using namespace indexlib::index;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SpatialTermQueryExecutor);

SpatialTermQueryExecutor::SpatialTermQueryExecutor(indexlib::index::PostingIterator *iter,
                                                   const common::Term &term)
    : TermQueryExecutor(iter, term)
    , _filter(NULL)
    , _testDocCount(0) {
    initSpatialPostingIterator();
}

SpatialTermQueryExecutor::~SpatialTermQueryExecutor() {}

uint32_t SpatialTermQueryExecutor::getSeekDocCount() {
    return _spatialIter->GetSeekDocCount() + _testDocCount;
}

indexlib::index::ErrorCode SpatialTermQueryExecutor::doSeek(docid_t id, docid_t &result) {
    docid_t tempDocId = INVALID_DOCID;
    auto ec = _spatialIter->InnerSeekDoc(id, tempDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    if (_filter) {
        while (tempDocId != INVALID_DOCID) {
            if (_filter->Test(tempDocId)) {
                _testDocCount++;
                break;
            }
            auto ec = _spatialIter->InnerSeekDoc(tempDocId, tempDocId);
            IE_RETURN_CODE_IF_ERROR(ec);
        }
    }
    if (tempDocId == INVALID_DOCID) {
        tempDocId = END_DOCID;
    }
    result = tempDocId;
    return indexlib::index::ErrorCode::OK;
}

void SpatialTermQueryExecutor::initSpatialPostingIterator() {
    indexlib::index::SeekAndFilterIterator *compositeIter
        = (indexlib::index::SeekAndFilterIterator *)_iter;
    _spatialIter = dynamic_cast<indexlib::index::SpatialPostingIterator *>(
        compositeIter->GetIndexIterator());
    _filter = compositeIter->GetDocValueFilter();
}

} // namespace search
} // namespace isearch
