#include <ha3_sdk/testlib/index/FakeInDocPositionState.h>
#include <ha3_sdk/testlib/index/FakeInDocPositionIterator.h>

using namespace autil;

IE_NAMESPACE_BEGIN(index);
HA3_LOG_SETUP(index, FakeInDocPositionState);

FakeInDocPositionState::
FakeInDocPositionState()
{
}

InDocPositionIterator* FakeInDocPositionState::CreateInDocIterator() const {
    return new FakeInDocPositionIterator(_posting.occArray,
        _posting.fieldBitArray, _posting.sectionIdArray);
}

InDocPositionState* FakeInDocPositionState::Clone() const {
    return new FakeInDocPositionState(*this);
}

IE_NAMESPACE_END(index);

