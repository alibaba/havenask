#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator_state.h"
#include "indexlib/index/normal/inverted_index/format/position_list_segment_decoder.h"

using namespace std;


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InDocPositionIteratorState);

tf_t InDocPositionIteratorState::GetTermFreq()
{
    if (mTermFreq == 0 && mOption.HasTfBitmap())
    {
        PositionBitmapReader* tfBitmap = mPosSegDecoder->GetPositionBitmapReader();
        PosCountInfo info = tfBitmap->GetPosCountInfo(GetSeekedDocCount());
        mTermFreq = info.currentDocPosCount;
    }
    return mTermFreq;
}

IE_NAMESPACE_END(index);

