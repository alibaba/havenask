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
#include "indexlib/index/inverted_index/MultiSegmentPostingIterator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, MultiSegmentPostingIterator);

MultiSegmentPostingIterator::MultiSegmentPostingIterator() : _cursor(0) {}

MultiSegmentPostingIterator::~MultiSegmentPostingIterator()
{
    // fileWriterPtr->Close().GetOrThrow(); //todo
}

index::ErrorCode MultiSegmentPostingIterator::SeekDocWithErrorCode(docid64_t docId, docid64_t& result)
{
    if (unlikely(_cursor >= _size)) {
        result = INVALID_DOCID;
        return index::ErrorCode::OK;
    }
    while (_cursor < _size) {
        if (_cursor + 1 < _size && _postingIterators[_cursor + 1].first <= docId) {
            ++_cursor;
            continue;
        }

        docid32_t localDocid = docId - _postingIterators[_cursor].first;
        auto errorCode = _postingIterators[_cursor].second->SeekDocWithErrorCode(localDocid, result);
        if (errorCode != index::ErrorCode::OK) {
            return errorCode;
        }

        if (result != INVALID_DOCID) {
            return errorCode;
        }

        _cursor++;
    }
    result = INVALID_DOCID;
    return index::ErrorCode::OK;
}

docid64_t MultiSegmentPostingIterator::SeekDoc(docid64_t docId)
{
    if (unlikely(_cursor >= _size)) {
        return INVALID_DOCID;
    }
    while (_cursor < _size) {
        if (_cursor + 1 < _size && _postingIterators[_cursor + 1].first <= docId) {
            ++_cursor;
            continue;
        }

        docid32_t localDocid = docId - _postingIterators[_cursor].first;
        docid32_t docid = _postingIterators[_cursor].second->SeekDoc(localDocid);
        if (docid != INVALID_DOCID) {
            return _postingIterators[_cursor].first + docid;
        }
        _cursor++;
    }
    return INVALID_DOCID;
}

void MultiSegmentPostingIterator::Init(std::vector<PostingIteratorInfo> postingIterators,
                                       std::vector<segmentid_t> segmentIdxs,
                                       std::vector<file_system::InterimFileWriterPtr> fileWriters)
{
    assert(postingIterators.size() == fileWriters.size());
    _postingIterators = postingIterators;
    _size = postingIterators.size();
    _fileWriters = fileWriters;
    _segmentIdxs = segmentIdxs;
}

TermMeta* MultiSegmentPostingIterator::GetTermMeta() const
{
    assert(false);
    return _postingIterators[_cursor].second->GetTermMeta();
}
TermMeta* MultiSegmentPostingIterator::GetTruncateTermMeta() const
{
    assert(false);
    return NULL;
}
TermPostingInfo MultiSegmentPostingIterator::GetTermPostingInfo() const
{
    assert(false);
    return _postingIterators[_cursor].second->GetTermPostingInfo();
}
autil::mem_pool::Pool* MultiSegmentPostingIterator::GetSessionPool() const
{
    assert(false);
    return NULL;
    // return _postingIterators[0]->GetSessionPool();
}
} // namespace indexlib::index
