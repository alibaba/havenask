#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_in_doc_position_state.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_in_doc_position_iterator.h"

IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, BitmapInDocPositionState);

BitmapInDocPositionState::BitmapInDocPositionState() 
    : InDocPositionState(NO_PAYLOAD)
    , mOwner(NULL)
{
}

BitmapInDocPositionState::~BitmapInDocPositionState() 
{
}

InDocPositionIterator* BitmapInDocPositionState::CreateInDocIterator() const
{
    BitmapInDocPositionIterator* iter =
        new BitmapInDocPositionIterator();
    return iter;
}

void BitmapInDocPositionState::Clone(BitmapInDocPositionState* state) const
{
    state->mOption = mOption;
    state->mDocId = mDocId;
    state->mTermFreq = mTermFreq;
}

void BitmapInDocPositionState::Free()
{
    if (mOwner)
    {
        mOwner->Free(this);
    }
}

IE_NAMESPACE_END(index);

