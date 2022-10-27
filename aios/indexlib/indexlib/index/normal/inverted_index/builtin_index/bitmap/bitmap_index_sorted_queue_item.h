#ifndef __INDEXLIB_BITMAP_INDEX_SORTED_QUEUE_ITEM_H
#define __INDEXLIB_BITMAP_INDEX_SORTED_QUEUE_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_decoder.h"

IE_NAMESPACE_BEGIN(index);

class BitmapIndexSortedQueueItem
{
public:
    BitmapIndexSortedQueueItem(const SegmentTermInfo& segTermInfo, 
                               const ReclaimMapPtr& reclaimMap);
    bool Next();
    docid_t GetDocId() const;
    const index::TermMeta *GetTermMeta() const;

private:
    bool MoveNextInDocBuffer();

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 1024 * 8;

    BitmapPostingDecoder* mBitmapDecoder;
    ReclaimMapPtr mReclaimMap;
    docid_t mBaseDocId;
    docid_t mNewDocId;

    docid_t mDocBuffer[DEFAULT_BUFFER_SIZE];
    int32_t mDecodedNum; 
    uint32_t mDocCursor;
    bool mEnd;
};

struct BitmapIndexSortedQueueItemComparator
{
    bool operator()(const BitmapIndexSortedQueueItem* item1, 
                    const BitmapIndexSortedQueueItem* item2)
    {
        return (item1->GetDocId() > item2->GetDocId()); 
    }
};

typedef std::priority_queue<BitmapIndexSortedQueueItem*, std::vector<BitmapIndexSortedQueueItem*>, 
                            BitmapIndexSortedQueueItemComparator> SortByWeightQueue;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_INDEX_SORTED_QUEUE_ITEM_H
