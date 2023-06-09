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
#include <memory>
#include <queue>

#include "indexlib/base/Define.h"
#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/inverted_index/format/BufferedSegmentIndexDecoder.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/util/SimpleHeap.h"

namespace indexlib::index {
class RangeSegmentPostingsIterator
{
public:
    RangeSegmentPostingsIterator(const PostingFormatOption& postingFormatOption, uint32_t& seekDocCounter,
                                 autil::mem_pool::Pool* sessionPool);
    ~RangeSegmentPostingsIterator();

private:
    struct PostingItem {
        docid_t docid;
        docid_t lastDocid;
        uint32_t bufferCursor;
        PostingItem(docid_t did, docid_t lastid, uint32_t cursor) : docid(did), lastDocid(lastid), bufferCursor(cursor)
        {
        }
    };
    class PostingItemComparator
    {
    public:
        bool operator()(const PostingItem& left, const PostingItem& right) const noexcept
        {
            return left.docid > right.docid;
        }
    };

    typedef util::SimpleHeap<PostingItem, PostingItemComparator> RangeHeap;

public:
    void Init(const std::shared_ptr<SegmentPostings>& segPostings, docid_t startDocid, docid_t nextSegmentDocId);
    index::Result<bool> Seek(docid_t docid) noexcept;
    docid_t GetCurrentDocid() const { return _currentDocId; }
    void Reset();

private:
    BufferedSegmentIndexDecoder* CreateSegmentDecoder(const SegmentPosting& segmentPosting,
                                                      file_system::ByteSliceReader* docListReaderPtr);
    BufferedSegmentIndexDecoder* CreateNormalSegmentDecoder(file_system::ByteSliceReader* docListReader,
                                                            uint8_t compressMode, uint32_t docListBeginPos,
                                                            PostingFormatOption& curSegPostingFormatOption);

private:
    autil::mem_pool::Pool* _sessionPool;
    uint32_t& _seekDocCounter;
    docid_t* _docBuffer;
    docid_t _currentDocId;
    docid_t _baseDocId;
    docid_t _nextSegmentDocId;
    uint32_t _bufferLength;
    PostingFormatOption _postingFormatOption;
    std::shared_ptr<SegmentPostings> _segPostings;
    std::vector<BufferedSegmentIndexDecoder*> _segmentDecoders;
    std::vector<file_system::ByteSliceReader> _docListReaders;
    RangeHeap _heap;

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////
__ALWAYS_INLINE inline index::Result<bool> RangeSegmentPostingsIterator::Seek(docid_t docid) noexcept
{
    if (_nextSegmentDocId != INVALID_DOCID && docid >= _nextSegmentDocId) {
        return false;
    }
    docid_t innerDocId = docid - _baseDocId;
    while (!_heap.empty()) {
        _seekDocCounter++;
        const PostingItem& item = _heap.top();
        if (item.docid >= innerDocId) {
            _currentDocId = item.docid + _baseDocId;
            return true;
        }

        PostingItem tmpItem = item;
        ttf_t currentTTF;
        if (unlikely(innerDocId > tmpItem.lastDocid)) {
            try {
                size_t bufferIdx = tmpItem.bufferCursor >> 7;
                if (!_segmentDecoders[bufferIdx]->DecodeDocBuffer(innerDocId, _docBuffer + (bufferIdx << 7),
                                                                  tmpItem.docid, tmpItem.lastDocid, currentTTF)) {
                    _heap.pop();
                    continue;
                }
                tmpItem.bufferCursor = (bufferIdx << 7);
            } catch (const util::FileIOException& e) {
                return false;
            }
        }
        docid_t* cursor = _docBuffer + tmpItem.bufferCursor;
        docid_t curDocId = tmpItem.docid;
        while (curDocId < innerDocId) {
            curDocId += *(++cursor);
        }
        tmpItem.bufferCursor = cursor - _docBuffer;
        tmpItem.docid = curDocId;
        if (tmpItem.docid - 1 == item.docid) {
            *(_heap.mutableTop()) = tmpItem;
        } else {
            _heap.updateTop([&tmpItem](PostingItem* item) { *item = tmpItem; });
        }
    }
    return false;
}
} // namespace indexlib::index
