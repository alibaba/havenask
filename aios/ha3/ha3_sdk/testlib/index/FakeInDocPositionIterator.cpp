#include "ha3_sdk/testlib/index/FakeInDocPositionIterator.h"

#include <assert.h>
#include <cstdint>

#include "autil/Log.h"

namespace indexlib {
namespace index {
AUTIL_LOG_SETUP(ha3, FakeInDocPositionIterator);

/*FakeInDocPositionIterator::
FakeInDocPositionIterator(const std::vector<pos_t> &occArray,
                          const std::vector<int32_t> &fieldBitArray)
    : _occArray(occArray),_fieldBitArray(fieldBitArray)
{
    _offset = -1;
}
*/
FakeInDocPositionIterator::FakeInDocPositionIterator(const std::vector<pos_t> &occArray,
                                                     const std::vector<int32_t> &fieldBitArray,
                                                     const std::vector<sectionid_t> &sectionIdArray)
    : _occArray(occArray)
    , _fieldBitArray(fieldBitArray)
    , _sectionIdArray(sectionIdArray) {
    _offset = -1;
}

FakeInDocPositionIterator::~FakeInDocPositionIterator() {}

pos_t FakeInDocPositionIterator::SeekPosition(pos_t pos) {
    assert(_offset <= (int32_t)_occArray.size());
    while (++_offset < (int32_t)_occArray.size()) {
        if (_occArray[_offset] >= pos) {
            return _occArray[_offset];
        }
    }
    return INVALID_POSITION;
}

pospayload_t FakeInDocPositionIterator::GetPosPayload() {
    assert(false);
    return (pospayload_t)0;
}

sectionid_t FakeInDocPositionIterator::GetSectionId() {
    return _sectionIdArray[_offset];
}

section_len_t FakeInDocPositionIterator::GetSectionLength() {
    assert(false);
    return (section_len_t)0;
}

fieldid_t FakeInDocPositionIterator::GetFieldId() {
    assert(false);
    return (int32_t)0;
    // return _fieldBitArray[_offset];
}

section_weight_t FakeInDocPositionIterator::GetSectionWeight() {
    assert(false);
    return (section_weight_t)0;
}

int32_t FakeInDocPositionIterator::GetFieldPosition() {
    return _fieldBitArray[_offset];
}

bool FakeInDocPositionIterator::HasPosPayload() const {
    assert(false);
    return true;
}

} // namespace index
} // namespace indexlib
