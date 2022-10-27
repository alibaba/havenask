#ifndef __INDEXLIB_PERCENT_ADAPTIVE_BITMAP_TRIGGER_H
#define __INDEXLIB_PERCENT_ADAPTIVE_BITMAP_TRIGGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger.h"

IE_NAMESPACE_BEGIN(index);

class PercentAdaptiveBitmapTrigger : public AdaptiveBitmapTrigger
{
public:
    PercentAdaptiveBitmapTrigger(uint32_t totalDocCount,
                                 int32_t percent);
    ~PercentAdaptiveBitmapTrigger();

public:
    bool MatchAdaptiveBitmap(
            const PostingMergerPtr& postingMerger) override;

private:
    uint32_t mTotalDocCount;
    int32_t mPercent;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PercentAdaptiveBitmapTrigger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PERCENT_ADAPTIVE_BITMAP_TRIGGER_H
