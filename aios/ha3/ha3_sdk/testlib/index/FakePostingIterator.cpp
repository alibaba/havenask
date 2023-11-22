#include "ha3_sdk/testlib/index/FakePostingIterator.h"

#include <assert.h>

#include "autil/Log.h"
#include "ha3_sdk/testlib/index/FakeInDocPositionState.h"
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/util/ObjectPool.h"

namespace indexlib {
namespace index {

AUTIL_LOG_SETUP(ha3, FakePostingIterator);

docid64_t FakePostingIterator::SeekDoc(docid64_t id) {
    while (++_pos < (int32_t)_richPostings.second.size()) {
        AUTIL_LOG(TRACE3, "id: %ld, _pos: %d]", id, _pos);
        if (_richPostings.second[_pos].docid >= id) {
            _occPos = -1;
            return _richPostings.second[_pos].docid;
        }
    }
    return INVALID_DOCID;
}

pos_t FakePostingIterator::SeekPosition(pos_t occ) {
    assert(_occPos <= (int32_t)_richPostings.second[_pos].occArray.size());
    while (++_occPos < (int32_t)_richPostings.second[_pos].occArray.size()) {
        AUTIL_LOG(TRACE3, "occ: %d, _occPos: %d]", occ, _occPos);
        if (_richPostings.second[_pos].occArray[_occPos] >= occ) {
            return _richPostings.second[_pos].occArray[_occPos];
        }
    }
    return INVALID_POSITION;
}

void FakePostingIterator::Unpack(TermMatchData &tmd) {
    FakeInDocPositionState *state = _objectPool.Alloc();
    state->init(_richPostings.second[_pos]);
    uint32_t tf = _richPostings.second[_pos].occArray.size();
    state->SetTermFreq((tf_t)tf);
    tmd.SetInDocPositionState(state);

    if (_richPostings.second[_pos].occArray.size() > 0) {
        tmd.SetFirstOcc(_richPostings.second[_pos].occArray[0]);
    }
    uint32_t fieldBitMap = 0;
    uint32_t fieldBitArraySize = _richPostings.second[_pos].fieldBitArray.size();
    for (uint32_t i = 0; i < tf && i < fieldBitArraySize; i++) {
        fieldBitMap |= (1 << _richPostings.second[_pos].fieldBitArray[i]);
    }
    tmd.SetFieldMap(fieldBitMap);
    tmd.SetDocPayload((docpayload_t)_richPostings.second[_pos].docPayload);
}

indexlib::matchvalue_t FakePostingIterator::GetMatchValue() const {
    indexlib::matchvalue_t mRes;
    mRes.SetUint16(_richPostings.second[_pos].docPayload);
    return mRes;
}

} // namespace index
} // namespace indexlib
