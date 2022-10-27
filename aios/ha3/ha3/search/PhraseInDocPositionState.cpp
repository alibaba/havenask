#include <ha3/search/PhraseInDocPositionState.h>

using namespace std;
IE_NAMESPACE_BEGIN(index);
HA3_LOG_SETUP(search, PhraseInDocPositionState);

PhraseInDocPositionState::PhraseInDocPositionState(vector<pos_t> *posVector, tf_t tf) 
    : _posVectorPtr(posVector)
{
    SetTermFreq(tf);
}

PhraseInDocPositionState::
PhraseInDocPositionState(const PhraseInDocPositionState &state)
{
    _posVectorPtr = state._posVectorPtr;
    SetTermFreq(state.GetTermFreq());
}

InDocPositionIterator* PhraseInDocPositionState:: CreateInDocIterator() const
{
    return new PhraseInDocPositionIterator(_posVectorPtr);
}

InDocPositionState* PhraseInDocPositionState::Clone() const
{
    return new PhraseInDocPositionState(*this);
}

IE_NAMESPACE_END(index);

