#include "indexlib/index/normal/adaptive_bitmap/df_adaptive_bitmap_trigger.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DfAdaptiveBitmapTrigger);

DfAdaptiveBitmapTrigger::DfAdaptiveBitmapTrigger(int32_t threshold)
    : mThreshold(threshold)
{
}

DfAdaptiveBitmapTrigger::~DfAdaptiveBitmapTrigger() 
{
}

bool DfAdaptiveBitmapTrigger::MatchAdaptiveBitmap(
        const PostingMergerPtr& postingMerger)
{
    df_t df = postingMerger->GetDocFreq();
    return df >= mThreshold;
}

IE_NAMESPACE_END(index);

