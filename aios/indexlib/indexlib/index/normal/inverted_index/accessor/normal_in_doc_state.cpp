#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_state.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_position_iterator.h"

IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalInDocState);

NormalInDocState::NormalInDocState(uint8_t compressMode,
                                   const PositionListFormatOption& option)
    : InDocPositionIteratorState(compressMode, option)
    , mOwner(NULL)
{}

NormalInDocState::NormalInDocState(const NormalInDocState& other)
    : InDocPositionIteratorState(other)
    , mOwner(other.mOwner)
{}

NormalInDocState::~NormalInDocState() 
{
}

InDocPositionIterator* NormalInDocState::CreateInDocIterator() const
{
    if (mOption.HasPositionList())
    {
        NormalInDocPositionIterator* iter =
            new NormalInDocPositionIterator(mOption);
        iter->Init(*this);
        return iter;
    }
    else
    {
        return NULL;
    }
}

void NormalInDocState::Clone(NormalInDocState* state) const
{
    InDocPositionIteratorState::Clone(state);
}

void NormalInDocState::Free()
{
    if (mOwner)
    {
        mOwner->Free(this);
    }
    mPosSegDecoder = NULL;
}

IE_NAMESPACE_END(index);

