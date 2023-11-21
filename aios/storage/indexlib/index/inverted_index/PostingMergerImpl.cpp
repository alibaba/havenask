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
#include "indexlib/index/inverted_index/PostingMergerImpl.h"

#include "indexlib/index/DocMapper.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/OneDocMerger.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PostingMergerImpl);

class PostingListSortedItem
{
public:
    PostingListSortedItem(const PostingFormatOption& formatOption, docid64_t baseDocId,
                          const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper, PostingDecoderImpl* decoder,
                          SingleTermIndexSegmentPatchIterator* patchIter)
        : _baseDocId(baseDocId)
        , _docMapper(docMapper)
        , _oneDocMerger(formatOption, decoder, patchIter)
    {
    }

public:
    bool Next()
    {
        bool hasNext = _oneDocMerger.Next();
        if (!hasNext) {
            return false;
        }
        _oldDocId = _baseDocId + _oneDocMerger.CurrentDoc();
        _current = _docMapper->Map(_oldDocId);
        return true;
    }

    void MergeDoc(const std::shared_ptr<MultiSegmentPostingWriter>& posWriter)
    {
        auto [newSegId, newLocalDocId] = _current;
        if (newLocalDocId == INVALID_DOCID) {
            assert(newSegId == INVALID_SEGMENTID);
            _oneDocMerger.Merge(INVALID_DOCID, nullptr);
            return;
        }
        auto postingWriterPtr =
            std::dynamic_pointer_cast<PostingWriterImpl>(posWriter->GetSegmentPostingWriterBySegId(newSegId));
        assert(postingWriterPtr);
        _oneDocMerger.Merge(newLocalDocId, postingWriterPtr.get());
    }

    std::pair<segmentid_t, docid32_t> GetCurrent() const { return _current; }

private:
    docid64_t _baseDocId;
    docid64_t _oldDocId = INVALID_DOCID;
    std::pair<segmentid_t, docid32_t> _current = {INVALID_SEGMENTID, INVALID_DOCID};
    std::shared_ptr<indexlibv2::index::DocMapper> _docMapper;
    OneDocMerger _oneDocMerger;
};

struct PostingListSortedItemComparator {
    bool operator()(const PostingListSortedItem* item1, const PostingListSortedItem* item2)
    {
        return (item1->GetCurrent() > item2->GetCurrent());
    }
};

using PriorityQueue =
    std::priority_queue<PostingListSortedItem*, std::vector<PostingListSortedItem*>, PostingListSortedItemComparator>;

PostingMergerImpl::PostingMergerImpl(
    PostingWriterResource* postingWriterResource,
    const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments)
    : _df(-1)
    , _ttf(-1)
{
    assert(postingWriterResource);
    _postingFormatOption = postingWriterResource->postingFormatOption;
    _postingWriter = CreatePostingWriter(postingWriterResource, targetSegments);
}

std::shared_ptr<MultiSegmentPostingWriter> PostingMergerImpl::CreatePostingWriter(
    PostingWriterResource* postingWriterResource,
    const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments)
{
    return std::make_shared<MultiSegmentPostingWriter>(postingWriterResource, targetSegments, _postingFormatOption);
}

void PostingMergerImpl::Merge(const SegmentTermInfos& segTermInfos,
                              const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper)
{
    PriorityQueue queue;
    for (size_t i = 0; i < segTermInfos.size(); ++i) {
        SegmentTermInfo* segInfo = segTermInfos[i];
        docid64_t baseDocId = segInfo->GetBaseDocId();
        auto decoder = dynamic_cast<PostingDecoderImpl*>(segInfo->GetPosting().first);
        _termPayload = 0;
        if (decoder) {
            _termPayload = decoder->GetTermMeta()->GetPayload();
        }

        PostingListSortedItem* queueItem = new PostingListSortedItem(_postingFormatOption, baseDocId, docMapper,
                                                                     decoder, segInfo->GetPosting().second);
        if (queueItem->Next()) {
            queue.push(queueItem);
        } else {
            delete queueItem;
        }
    }

    while (!queue.empty()) {
        PostingListSortedItem* item = queue.top();
        queue.pop();
        item->MergeDoc(_postingWriter);
        if (item->Next()) {
            queue.push(item);
        } else {
            delete item;
        }
    }
    _postingWriter->EndSegment();
}

void PostingMergerImpl::Dump(
    const index::DictKeyInfo& key,
    const std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources)
{
    _postingWriter->Dump(key, indexOutputSegmentResources, _termPayload);
}

uint64_t PostingMergerImpl::GetPostingLength() const { return _postingWriter->GetPostingLength(); }

} // namespace indexlib::index
