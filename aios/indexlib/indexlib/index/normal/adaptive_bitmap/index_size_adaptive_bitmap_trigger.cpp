#include "indexlib/index/normal/adaptive_bitmap/index_size_adaptive_bitmap_trigger.h"
#include "indexlib/util/bitmap.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexSizeAdaptiveBitmapTrigger);

IndexSizeAdaptiveBitmapTrigger::IndexSizeAdaptiveBitmapTrigger(
        uint32_t totalDocCount)
{
    mEstimateBitmapSize = Bitmap::GetDumpSize(totalDocCount);
}

IndexSizeAdaptiveBitmapTrigger::~IndexSizeAdaptiveBitmapTrigger() 
{
}

bool IndexSizeAdaptiveBitmapTrigger::MatchAdaptiveBitmap(
        const PostingMergerPtr& postingMerger)
{
    uint32_t postingSize = postingMerger->GetPostingLength();
    return postingSize > mEstimateBitmapSize;
}

IE_NAMESPACE_END(index);

