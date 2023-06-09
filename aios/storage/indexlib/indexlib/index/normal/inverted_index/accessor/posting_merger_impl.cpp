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
#include "indexlib/index/normal/inverted_index/accessor/posting_merger_impl.h"

#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/OneDocMerger.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"

using namespace std;
using namespace autil::mem_pool;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::util;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, PostingMergerImpl);

class PostingListSortedItem
{
public:
    PostingListSortedItem(const PostingFormatOption& formatOption, docid_t baseDocId, const ReclaimMapPtr& reclaimMap,
                          PostingDecoderImpl* decoder, SingleTermIndexSegmentPatchIterator* patchIter)
        : mBaseDocId(baseDocId)
        , mNewDocId(INVALID_DOCID)
        , mOrderId(INVALID_DOCID)
        , mReclaimMap(reclaimMap)
        , mOneDocMerger(formatOption, decoder, patchIter)
    {
    }

public:
    bool Next()
    {
        bool hasNext = mOneDocMerger.Next();
        if (!hasNext) {
            return false;
        }
        docid_t oldDocId = mBaseDocId + mOneDocMerger.CurrentDoc();
        mNewDocId = mReclaimMap->GetNewId(oldDocId);
        mOrderId = mReclaimMap->GetDocOrder(oldDocId);
        return true;
    }

    void MergeDoc(const MultiSegmentPostingWriterPtr& posWriter)
    {
        if (mNewDocId == INVALID_DOCID) {
            mOneDocMerger.Merge(INVALID_DOCID, NULL);
            return;
        }
        pair<segmentindex_t, docid_t> docidInfo = mReclaimMap->GetLocalId(mNewDocId);
        shared_ptr<PostingWriterImpl> postingWriterPtr =
            DYNAMIC_POINTER_CAST(PostingWriterImpl, posWriter->GetSegmentPostingWriter(docidInfo.first));
        mOneDocMerger.Merge(docidInfo.second, postingWriterPtr.get());
    }

    docid_t GetDocId() const { return mOrderId; }

private:
    docid_t mBaseDocId;
    docid_t mNewDocId;
    docid_t mOrderId;
    ReclaimMapPtr mReclaimMap;
    OneDocMerger mOneDocMerger;
};

struct PostingListSortedItemComparator {
    bool operator()(const PostingListSortedItem* item1, const PostingListSortedItem* item2)
    {
        return (item1->GetDocId() > item2->GetDocId());
    }
};

typedef priority_queue<PostingListSortedItem*, vector<PostingListSortedItem*>, PostingListSortedItemComparator>
    SortWeightQueue;

/////////////////////////////////////////////////////////////////////////////
PostingMergerImpl::PostingMergerImpl(PostingWriterResource* postingWriterResource,
                                     const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
    : mDf(-1)
    , mttf(-1)
{
    assert(postingWriterResource);
    mPostingFormatOption = postingWriterResource->postingFormatOption;
    mPostingWriter = CreatePostingWriter(postingWriterResource, outputSegmentMergeInfos);
}

MultiSegmentPostingWriterPtr
PostingMergerImpl::CreatePostingWriter(PostingWriterResource* postingWriterResource,
                                       const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    MultiSegmentPostingWriterPtr multiSegmentPostingWriterPtr(
        new MultiSegmentPostingWriter(postingWriterResource, outputSegmentMergeInfos, mPostingFormatOption));
    return multiSegmentPostingWriterPtr;
}

PostingMergerImpl::~PostingMergerImpl() {}

void PostingMergerImpl::Merge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap)
{
    for (size_t i = 0; i < segTermInfos.size(); i++) {
        SegmentTermInfo* segInfo = segTermInfos[i];
        docid_t baseDocId = segInfo->GetBaseDocId();
        auto postingPair = segInfo->GetPosting();
        PostingDecoderImpl* decoder = dynamic_cast<PostingDecoderImpl*>(postingPair.first);
        SingleTermIndexSegmentPatchIterator* termIter = postingPair.second;
        if (decoder) {
            mTermPayload = decoder->GetTermMeta()->GetPayload();
        } else {
            mTermPayload = 0;
        }

        MergeOneSegment(reclaimMap, decoder, termIter, baseDocId);
    }
    mPostingWriter->EndSegment();
}

void PostingMergerImpl::MergeOneSegment(const ReclaimMapPtr& reclaimMap, PostingDecoderImpl* decoder,
                                        SingleTermIndexSegmentPatchIterator* patchIter, docid_t baseDocId)
{
    OneDocMerger oneDocMerger(mPostingFormatOption, decoder, patchIter);
    while (true) {
        bool hasNext = oneDocMerger.Next();
        if (!hasNext) {
            break;
        }
        docid_t oldDocId = oneDocMerger.CurrentDoc();

        pair<segmentindex_t, docid_t> docidInfo = reclaimMap->GetNewLocalId(baseDocId + oldDocId);
        if (docidInfo.second == INVALID_DOCID) {
            oneDocMerger.Merge(INVALID_DOCID, NULL);
        } else {
            shared_ptr<PostingWriterImpl> postingWriter =
                DYNAMIC_POINTER_CAST(PostingWriterImpl, mPostingWriter->GetSegmentPostingWriter(docidInfo.first));
            oneDocMerger.Merge(docidInfo.second, postingWriter.get());
        }
    }
}

void PostingMergerImpl::SortByWeightMerge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap)
{
    SortWeightQueue queue;
    for (size_t i = 0; i < segTermInfos.size(); ++i) {
        SegmentTermInfo* segInfo = segTermInfos[i];
        docid_t baseDocId = segInfo->GetBaseDocId();
        PostingDecoderImpl* decoder = dynamic_cast<PostingDecoderImpl*>(segInfo->GetPosting().first);
        mTermPayload = 0;
        if (decoder) {
            mTermPayload = decoder->GetTermMeta()->GetPayload();
        }

        PostingListSortedItem* queueItem = new PostingListSortedItem(mPostingFormatOption, baseDocId, reclaimMap,
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
        item->MergeDoc(mPostingWriter);
        if (item->Next()) {
            queue.push(item);
        } else {
            delete item;
        }
    }
    mPostingWriter->EndSegment();
}

void PostingMergerImpl::Dump(const index::DictKeyInfo& key, IndexOutputSegmentResources& indexOutputSegmentResources)
{
    mPostingWriter->Dump(key, indexOutputSegmentResources, mTermPayload);
}

uint64_t PostingMergerImpl::GetPostingLength() const { return mPostingWriter->GetPostingLength(); }
}}} // namespace indexlib::index::legacy
