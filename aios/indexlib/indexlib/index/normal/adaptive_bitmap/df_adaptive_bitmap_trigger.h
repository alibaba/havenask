#ifndef __INDEXLIB_DF_ADAPTIVE_BITMAP_TRIGGER_H
#define __INDEXLIB_DF_ADAPTIVE_BITMAP_TRIGGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger.h"

IE_NAMESPACE_BEGIN(index);

class DfAdaptiveBitmapTrigger : public AdaptiveBitmapTrigger
{
public:
    DfAdaptiveBitmapTrigger(int32_t threshold);
    ~DfAdaptiveBitmapTrigger();

public:
    bool MatchAdaptiveBitmap(
            const PostingMergerPtr& postingMerger) override;

private:
    int32_t mThreshold;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DfAdaptiveBitmapTrigger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DF_ADAPTIVE_BITMAP_TRIGGER_H
