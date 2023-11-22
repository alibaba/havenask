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
#include "ha3/search/CompositeTermQueryExecutor.h"

#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/TermPostingInfo.h"

using namespace indexlib::index;
using namespace isearch::common;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(search, CompositeTermQueryExecutor);

CompositeTermQueryExecutor::CompositeTermQueryExecutor(PostingIterator *iter, const Term &term)
    : TermQueryExecutor(iter, term) {
    initCompositePostingIterator();
}

CompositeTermQueryExecutor::~CompositeTermQueryExecutor() {}

indexlib::index::ErrorCode CompositeTermQueryExecutor::doSeek(docid_t id, docid_t &result) {
    indexlib::docid64_t tempDocId = INVALID_DOCID;
    auto ec = _compositeIter->SeekDocWithErrorCode(id, tempDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    ++_seekDocCount;
    if (tempDocId == INVALID_DOCID) {
        tempDocId = END_DOCID;
    }
    result = tempDocId;
    return indexlib::index::ErrorCode::OK;
}

void CompositeTermQueryExecutor::initCompositePostingIterator() {
    _compositeIter = dynamic_cast<indexlib::index::BufferedCompositePostingIterator *>(_iter);
}

} // namespace search
} // namespace isearch
