#include "indexlib/index/normal/adaptive_bitmap/percent_adaptive_bitmap_trigger.h"
#include "indexlib/util/math_util.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PercentAdaptiveBitmapTrigger);

PercentAdaptiveBitmapTrigger::PercentAdaptiveBitmapTrigger(
        uint32_t totalDocCount, int32_t percent)
    : mTotalDocCount(totalDocCount)
    , mPercent(percent)
{
    assert(percent >= 0 && percent <= 100);
}

PercentAdaptiveBitmapTrigger::~PercentAdaptiveBitmapTrigger() 
{
}

bool PercentAdaptiveBitmapTrigger::MatchAdaptiveBitmap(
        const PostingMergerPtr& postingMerger)
{
    df_t df = postingMerger->GetDocFreq();
    int32_t threshold = MathUtil::MultiplyByPercentage(mTotalDocCount, mPercent);
    return df >= threshold;
}

IE_NAMESPACE_END(index);

