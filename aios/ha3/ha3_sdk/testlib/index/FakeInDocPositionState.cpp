#include "ha3_sdk/testlib/index/FakeInDocPositionState.h"

#include "autil/Log.h"
#include "autil/LongHashValue.h"
#include "ha3_sdk/testlib/index/FakeInDocPositionIterator.h"
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"

namespace indexlib {
namespace index {
class InDocPositionIterator;
} // namespace index
} // namespace indexlib

using namespace autil;

namespace indexlib {
namespace index {
AUTIL_LOG_SETUP(ha3, FakeInDocPositionState);

FakeInDocPositionState::FakeInDocPositionState() {}

InDocPositionIterator *FakeInDocPositionState::CreateInDocIterator() const {
    return new FakeInDocPositionIterator(
        _posting.occArray, _posting.fieldBitArray, _posting.sectionIdArray);
}

InDocPositionState *FakeInDocPositionState::Clone() const {
    return new FakeInDocPositionState(*this);
}

} // namespace index
} // namespace indexlib
