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
#include "ha3/search/SubTermQueryExecutor.h"

#include <assert.h>
#include <iosfwd>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "ha3/isearch.h"
#include "ha3/search/TermQueryExecutor.h"

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

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SubTermQueryExecutor);

SubTermQueryExecutor::SubTermQueryExecutor(indexlib::index::PostingIterator *iter,
                                           const common::Term &term,
                                           DocMapAttrIterator *mainToSubIter,
                                           DocMapAttrIterator *subToMainIter)
    : TermQueryExecutor(iter, term)
    , _curSubDocId(INVALID_DOCID)
    , _mainToSubIter(mainToSubIter)
    , _subToMainIter(subToMainIter) {
    _hasSubDocExecutor = true;
}

SubTermQueryExecutor::~SubTermQueryExecutor() {}

indexlib::index::ErrorCode SubTermQueryExecutor::doSeek(docid_t docId, docid_t &result) {
    docid_t subDocStartId = 0;
    if (likely(docId != 0)) {
        bool ret = _mainToSubIter->Seek(docId - 1, subDocStartId);
        assert(ret);
        (void)ret;
    }
    if (unlikely(INVALID_DOCID == subDocStartId)) {
        // building doc in rt: set sub join, but not set main join
        result = END_DOCID;
        return indexlib::index::ErrorCode::OK;
    }
    docid_t retDocId = INVALID_DOCID;
    docid_t subDocId = subDocStartId;
    do {
        auto ec = subDocSeek(subDocId, subDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (subDocId == END_DOCID) {
            break;
        }
        bool ret = _subToMainIter->Seek(subDocId, retDocId);
        assert(ret);
        (void)ret;
        subDocId++;
    } while (retDocId == INVALID_DOCID);

    if (unlikely(retDocId == INVALID_DOCID)) {
        retDocId = END_DOCID;
    }
    if (unlikely(retDocId < docId)) {
        AUTIL_LOG(ERROR,
                  "Invalid map between sub and main table."
                  "main doc[%d] map to start sub doc[%d], "
                  "seek sub doc[%d] map to main doc[%d].",
                  docId,
                  subDocStartId,
                  subDocId,
                  retDocId);
        retDocId = END_DOCID;
    }
    assert(retDocId >= docId);
    result = retDocId;
    return indexlib::index::ErrorCode::OK;
}

indexlib::index::ErrorCode SubTermQueryExecutor::seekSubDoc(
    docid_t docId, docid_t subDocId, docid_t subDocEnd, bool needSubMatchdata, docid_t &result) {
    auto ec = subDocSeek(subDocId, subDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    if (subDocId < subDocEnd) {
        result = subDocId;
        return indexlib::index::ErrorCode::OK;
    }
    result = END_DOCID;
    return indexlib::index::ErrorCode::OK;
}

bool SubTermQueryExecutor::isMainDocHit(docid_t docId) const {
    return false;
}

void SubTermQueryExecutor::reset() {
    TermQueryExecutor::reset();
    _curSubDocId = INVALID_DOCID;
}

string SubTermQueryExecutor::toString() const {
    return "sub_term:" + TermQueryExecutor::toString();
}

} // namespace search
} // namespace isearch
