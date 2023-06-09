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
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_sorted_queue_item.h"

#include "indexlib/index/inverted_index/SegmentTermInfo.h"

using namespace std;

namespace indexlib { namespace index { namespace legacy {

BitmapIndexSortedQueueItem::BitmapIndexSortedQueueItem(const SegmentTermInfo& segTermInfo,
                                                       const ReclaimMapPtr& reclaimMap)
    : mBitmapDecoder(NULL)
    , mReclaimMap(reclaimMap)
    , mBaseDocId(segTermInfo.GetBaseDocId())
    , mNewDocId(INVALID_DOCID)
    , mOrderDocId(INVALID_DOCID)
    , mDecodedNum(0)
    , mDocCursor(0)
    , mEnd(false)
{
    std::pair<PostingDecoder*, SingleTermIndexSegmentPatchIterator*> pair = segTermInfo.GetPosting();
    mBitmapDecoder = dynamic_cast<BitmapPostingDecoder*>(pair.first);
}

const TermMeta* BitmapIndexSortedQueueItem::GetTermMeta() const { return mBitmapDecoder->GetTermMeta(); }

bool BitmapIndexSortedQueueItem::MoveNextInDocBuffer()
{
    while (mDocCursor < (uint32_t)mDecodedNum) {
        docid_t oldId = mDocBuffer[mDocCursor] + mBaseDocId;
        mDocCursor++;
        mNewDocId = mReclaimMap->GetNewId(oldId);
        mOrderDocId = mReclaimMap->GetDocOrder(oldId);
        if (mNewDocId != INVALID_DOCID) {
            return true;
        }
    }
    return false;
}

bool BitmapIndexSortedQueueItem::Next()
{
    if (mEnd) {
        return false;
    }

    if (MoveNextInDocBuffer()) {
        return true;
    }

    if (!mBitmapDecoder) {
        return false;
    }
    while (1) {
        mDecodedNum = mBitmapDecoder->DecodeDocList(mDocBuffer, DEFAULT_BUFFER_SIZE);
        if (mDecodedNum <= 0) {
            mEnd = true;
            return false;
        }

        mDocCursor = 0;
        if (MoveNextInDocBuffer()) {
            return true;
        }
    }
}

docid_t BitmapIndexSortedQueueItem::GetDocId() const { return mOrderDocId; }
}}} // namespace indexlib::index::legacy
