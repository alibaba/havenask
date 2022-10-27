#ifndef __INDEXLIB_INDEX_SIZE_ADAPTIVE_BITMAP_TRIGGER_H
#define __INDEXLIB_INDEX_SIZE_ADAPTIVE_BITMAP_TRIGGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger.h"

IE_NAMESPACE_BEGIN(index);

class IndexSizeAdaptiveBitmapTrigger : public AdaptiveBitmapTrigger
{
public:
    IndexSizeAdaptiveBitmapTrigger(uint32_t totalDocCount);
    ~IndexSizeAdaptiveBitmapTrigger();

public:
    bool MatchAdaptiveBitmap(
            const PostingMergerPtr& postingMerger) override;

private:
    uint32_t mEstimateBitmapSize;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSizeAdaptiveBitmapTrigger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_SIZE_ADAPTIVE_BITMAP_TRIGGER_H
