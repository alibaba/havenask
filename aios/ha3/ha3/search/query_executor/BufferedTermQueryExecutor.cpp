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
#include "ha3/search/BufferedTermQueryExecutor.h"

#include "autil/Log.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

using namespace indexlib::index;
using namespace isearch::common;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, BufferedTermQueryExecutor);

BufferedTermQueryExecutor::BufferedTermQueryExecutor(PostingIterator *iter, const Term &term)
    : TermQueryExecutor(iter, term) {
    initBufferedPostingIterator();
}

BufferedTermQueryExecutor::~BufferedTermQueryExecutor() {}

indexlib::index::ErrorCode BufferedTermQueryExecutor::doSeek(docid_t id, docid_t &result) {
    indexlib::docid64_t tempDocId = INVALID_DOCID;
    auto ec = _bufferedIter->InnerSeekDoc(id, tempDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    ++_seekDocCount;
    if (tempDocId == INVALID_DOCID) {
        tempDocId = END_DOCID;
    }
    result = tempDocId;
    return indexlib::index::ErrorCode::OK;
}

void BufferedTermQueryExecutor::initBufferedPostingIterator() {
    _bufferedIter = dynamic_cast<indexlib::index::BufferedPostingIterator *>(_iter);
}

} // namespace search
} // namespace isearch
