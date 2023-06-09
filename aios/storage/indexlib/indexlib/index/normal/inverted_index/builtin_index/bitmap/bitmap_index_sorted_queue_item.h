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
#ifndef __INDEXLIB_BITMAP_INDEX_SORTED_QUEUE_ITEM_H
#define __INDEXLIB_BITMAP_INDEX_SORTED_QUEUE_ITEM_H

#include <memory>
#include <queue>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDecoder.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, SegmentTermInfo);

namespace indexlib { namespace index { namespace legacy {

class BitmapIndexSortedQueueItem
{
public:
    BitmapIndexSortedQueueItem(const SegmentTermInfo& segTermInfo, const ReclaimMapPtr& reclaimMap);
    bool Next();
    docid_t GetDocId() const;
    const TermMeta* GetTermMeta() const;

private:
    bool MoveNextInDocBuffer();

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 1024 * 8;

    BitmapPostingDecoder* mBitmapDecoder;
    ReclaimMapPtr mReclaimMap;
    docid_t mBaseDocId;
    docid_t mNewDocId;
    docid_t mOrderDocId;

    docid_t mDocBuffer[DEFAULT_BUFFER_SIZE];
    int32_t mDecodedNum;
    uint32_t mDocCursor;
    bool mEnd;
};

struct BitmapIndexSortedQueueItemComparator {
    bool operator()(const BitmapIndexSortedQueueItem* item1, const BitmapIndexSortedQueueItem* item2)
    {
        return (item1->GetDocId() > item2->GetDocId());
    }
};

typedef std::priority_queue<BitmapIndexSortedQueueItem*, std::vector<BitmapIndexSortedQueueItem*>,
                            BitmapIndexSortedQueueItemComparator>
    SortByWeightQueue;
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_BITMAP_INDEX_SORTED_QUEUE_ITEM_H
